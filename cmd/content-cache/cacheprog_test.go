package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestRunner creates a cacheprogRunner wired to a test HTTP server
// and local temp directory, with stdout captured to a buffer.
func newTestRunner(t *testing.T, serverURL string) (*cacheprogRunner, *bytes.Buffer) {
	t.Helper()
	localDir := t.TempDir()
	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)
	return &cacheprogRunner{
		serverURL:  serverURL,
		localDir:   localDir,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		bw:         bw,
		enc:        json.NewEncoder(bw),
	}, &buf
}

func TestHandleGetMiss(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	runner, _ := newTestRunner(t, srv.URL)
	actionID, _ := hex.DecodeString("aa" + "00000000000000000000000000000000000000000000000000000000000000")
	resp := runner.handleGet(t.Context(), progRequest{ID: 1, ActionID: actionID})

	require.True(t, resp.Miss)
	require.Equal(t, int64(1), resp.ID)
}

func TestHandleGetHit(t *testing.T) {
	blobContent := "cached build output"
	outputIDHex := "bb00000000000000000000000000000000000000000000000000000000000000"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Output-ID", outputIDHex)
		_, _ = w.Write([]byte(blobContent))
	}))
	defer srv.Close()

	runner, _ := newTestRunner(t, srv.URL)
	actionID, _ := hex.DecodeString("aa00000000000000000000000000000000000000000000000000000000000000")
	resp := runner.handleGet(t.Context(), progRequest{ID: 2, ActionID: actionID})

	require.False(t, resp.Miss)
	require.Equal(t, int64(2), resp.ID)
	require.Equal(t, int64(len(blobContent)), resp.Size)
	require.NotEmpty(t, resp.DiskPath)
	require.NotNil(t, resp.Time)

	// Verify the OutputID was decoded correctly.
	require.Equal(t, outputIDHex, hex.EncodeToString(resp.OutputID))

	// Verify blob was written to disk.
	data, err := os.ReadFile(resp.DiskPath)
	require.NoError(t, err)
	require.Equal(t, blobContent, string(data))
}

func TestHandleGetLocalCacheHit(t *testing.T) {
	// Seed the local cache so the network is never hit.
	runner, _ := newTestRunner(t, "http://should-not-be-called")

	actionHex := "cc00000000000000000000000000000000000000000000000000000000000000"
	outputIDHex := "dd00000000000000000000000000000000000000000000000000000000000000"
	localFile := filepath.Join(runner.localDir, actionHex)
	metaFile := localFile + ".meta"

	require.NoError(t, os.WriteFile(localFile, []byte("local blob"), 0o600))
	now := time.Now()
	meta := localMeta{OutputID: outputIDHex, Size: 10, Time: &now}
	metaData, _ := json.Marshal(meta)
	require.NoError(t, os.WriteFile(metaFile, metaData, 0o600))

	actionID, _ := hex.DecodeString(actionHex)
	resp := runner.handleGet(t.Context(), progRequest{ID: 3, ActionID: actionID})

	require.False(t, resp.Miss)
	require.Equal(t, int64(3), resp.ID)
	require.Equal(t, localFile, resp.DiskPath)
	require.Equal(t, int64(10), resp.Size)
}

func TestHandlePutSuccess(t *testing.T) {
	var receivedBody []byte
	var receivedOutputID string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedOutputID = r.URL.Query().Get("output_id")
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	runner, _ := newTestRunner(t, srv.URL)
	actionID, _ := hex.DecodeString("ee00000000000000000000000000000000000000000000000000000000000000")
	outputID, _ := hex.DecodeString("ff00000000000000000000000000000000000000000000000000000000000000")
	body := []byte("build artifact data")

	resp := runner.handlePut(t.Context(), progRequest{
		ID:       4,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, body)

	require.Empty(t, resp.Err)
	require.Equal(t, int64(4), resp.ID)
	require.NotEmpty(t, resp.DiskPath)

	// Verify the server received the correct data.
	require.Equal(t, string(body), string(receivedBody))
	require.Equal(t, hex.EncodeToString(outputID), receivedOutputID)

	// Verify local file was written.
	data, err := os.ReadFile(resp.DiskPath)
	require.NoError(t, err)
	require.Equal(t, string(body), string(data))

	// Verify sidecar was written with Time set.
	metaFile := resp.DiskPath + ".meta"
	metaData, err := os.ReadFile(metaFile)
	require.NoError(t, err)
	var meta localMeta
	require.NoError(t, json.Unmarshal(metaData, &meta))
	require.NotNil(t, meta.Time)
	require.Equal(t, int64(len(body)), meta.Size)
}

func TestHandlePutUploadFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	runner, _ := newTestRunner(t, srv.URL)
	actionID, _ := hex.DecodeString("aa00000000000000000000000000000000000000000000000000000000000000")
	outputID, _ := hex.DecodeString("bb00000000000000000000000000000000000000000000000000000000000000")

	resp := runner.handlePut(t.Context(), progRequest{
		ID:       5,
		ActionID: actionID,
		OutputID: outputID,
	}, []byte("data"))

	require.Contains(t, resp.Err, "upload returned status 500")
}

func TestRunProtocol(t *testing.T) {
	// Test the full stdin/stdout protocol loop with just a close command
	// to verify the capability handshake and graceful shutdown.
	localDir := t.TempDir()

	// Build the input stream: just a close command.
	var input bytes.Buffer
	enc := json.NewEncoder(&input)
	_ = enc.Encode(progRequest{ID: 1, Command: "close"})

	// Redirect stdin/stdout for the runner.
	origStdin := os.Stdin
	origStdout := os.Stdout
	defer func() {
		os.Stdin = origStdin
		os.Stdout = origStdout
	}()

	stdinR, stdinW, _ := os.Pipe()
	stdoutR, stdoutW, _ := os.Pipe()

	os.Stdin = stdinR
	os.Stdout = stdoutW

	// Write input in a goroutine.
	go func() {
		_, _ = stdinW.Write(input.Bytes())
		_ = stdinW.Close()
	}()

	bw := bufio.NewWriter(stdoutW)
	runner := &cacheprogRunner{
		serverURL:  "http://localhost:0",
		localDir:   localDir,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		bw:         bw,
		enc:        json.NewEncoder(bw),
	}

	err := runner.run()
	_ = stdoutW.Close()
	require.NoError(t, err)

	// Read and verify responses.
	var responses []progResponse
	dec := json.NewDecoder(stdoutR)
	for {
		var resp progResponse
		if err := dec.Decode(&resp); err != nil {
			break
		}
		responses = append(responses, resp)
	}

	require.Len(t, responses, 2) // capability + close
	require.Equal(t, int64(0), responses[0].ID)
	require.Contains(t, responses[0].KnownCommands, "get")
	require.Contains(t, responses[0].KnownCommands, "put")
	require.Contains(t, responses[0].KnownCommands, "close")
	require.Equal(t, int64(1), responses[1].ID)
}

func TestRunProtocolEOF(t *testing.T) {
	// Test that the runner exits cleanly on EOF (stdin closed without close command).
	localDir := t.TempDir()

	origStdin := os.Stdin
	origStdout := os.Stdout
	defer func() {
		os.Stdin = origStdin
		os.Stdout = origStdout
	}()

	stdinR, stdinW, _ := os.Pipe()
	_, stdoutW, _ := os.Pipe()

	os.Stdin = stdinR
	os.Stdout = stdoutW

	// Close stdin immediately after runner starts reading.
	go func() {
		_ = stdinW.Close()
	}()

	bw := bufio.NewWriter(stdoutW)
	runner := &cacheprogRunner{
		serverURL:  "http://localhost:0",
		localDir:   localDir,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		bw:         bw,
		enc:        json.NewEncoder(bw),
	}

	err := runner.run()
	_ = stdoutW.Close()
	require.NoError(t, err)
}
