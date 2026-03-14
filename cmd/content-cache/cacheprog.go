package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// CacheprogCmd runs the binary as a GOCACHEPROG subprocess.
// The go tool communicates with it over stdin/stdout using JSON.
//
// Usage:
//
//	GOCACHEPROG="content-cache cacheprog --server http://localhost:8080" go build ./...
type CacheprogCmd struct {
	Server   string `kong:"name='server',required,env='CONTENT_CACHE_SERVER',help='content-cache server URL (e.g. http://localhost:8080)'"`
	LocalDir string `kong:"name='local-dir',env='CONTENT_CACHE_LOCAL_DIR',help='local directory for artifact files (default: <user-cache-dir>/go/buildcache)'"`
}

func (cmd *CacheprogCmd) Run() error {
	localDir := cmd.LocalDir
	if localDir == "" {
		cacheDir, err := os.UserCacheDir()
		if err != nil {
			return fmt.Errorf("getting user cache dir: %w", err)
		}
		localDir = filepath.Join(cacheDir, "go", "buildcache")
	}
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return fmt.Errorf("creating local dir %s: %w", localDir, err)
	}

	bw := bufio.NewWriter(os.Stdout)
	r := &cacheprogRunner{
		serverURL:  cmd.Server,
		localDir:   localDir,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		bw:         bw,
		enc:        json.NewEncoder(bw),
	}
	return r.run()
}

// progRequest mirrors cmd/go/internal/cacheprog.ProgRequest.
// ActionID and OutputID are []byte so they arrive base64-encoded in JSON.
type progRequest struct {
	ID       int64  `json:"ID"`
	Command  string `json:"Command"`
	ActionID []byte `json:"ActionID"`
	OutputID []byte `json:"OutputID"`
	ObjectID []byte `json:"ObjectID"` // Deprecated: renamed to OutputID in Go 1.24; kept for Go 1.21–1.23 compat
	BodySize int64  `json:"BodySize"`
}

// progResponse mirrors cmd/go/internal/cacheprog.ProgResponse.
type progResponse struct {
	ID            int64      `json:"ID"`
	Err           string     `json:"Err,omitempty"`
	KnownCommands []string   `json:"KnownCommands,omitempty"`
	Miss          bool       `json:"Miss,omitempty"`
	OutputID      []byte     `json:"OutputID,omitempty"`
	Size          int64      `json:"Size,omitempty"`
	Time          *time.Time `json:"Time,omitempty"`
	DiskPath      string     `json:"DiskPath,omitempty"`
}

// localMeta is stored as a sidecar alongside each locally cached blob.
type localMeta struct {
	OutputID string     `json:"output_id"` // hex-encoded
	Size     int64      `json:"size"`
	Time     *time.Time `json:"time"`
}

type cacheprogRunner struct {
	serverURL  string
	localDir   string
	httpClient *http.Client
	bw         *bufio.Writer
	enc        *json.Encoder
	mu         sync.Mutex // guards enc and bw
	cancel     context.CancelFunc
}

func (r *cacheprogRunner) writeResponse(res progResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.enc.Encode(res); err != nil {
		return err
	}
	return r.bw.Flush()
}

func (r *cacheprogRunner) run() error {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	// Announce supported commands. ID==0 is the capability handshake.
	if err := r.writeResponse(progResponse{
		ID:            0,
		KnownCommands: []string{"get", "put", "close"},
	}); err != nil {
		cancel()
		return fmt.Errorf("writing capability response: %w", err)
	}

	// The go tool sends requests as JSON. For "put" requests the body follows
	// as a second JSON value (a base64-encoded []byte), not as raw bytes.
	dec := json.NewDecoder(bufio.NewReader(os.Stdin))

	for {
		var req progRequest
		if err := dec.Decode(&req); err != nil {
			cancel()
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("decoding request: %w", err)
		}

		// Go 1.24 backward compat: ObjectID was renamed to OutputID.
		if len(req.OutputID) == 0 && len(req.ObjectID) != 0 {
			req.OutputID = req.ObjectID
		}

		switch req.Command {
		case "get":
			go func(req progRequest) {
				if err := r.writeResponse(r.handleGet(ctx, req)); err != nil {
					fmt.Fprintf(os.Stderr, "cacheprog: write response for get %d: %v\n", req.ID, err)
				}
			}(req)

		case "put":
			// Body is sent as a second JSON value (base64-encoded []byte).
			// Must be read synchronously before launching the goroutine.
			var body []byte
			if req.BodySize > 0 {
				if err := dec.Decode(&body); err != nil {
					_ = r.writeResponse(progResponse{ID: req.ID, Err: fmt.Sprintf("decoding body: %v", err)})
					continue
				}
			}
			go func(req progRequest, body []byte) {
				if err := r.writeResponse(r.handlePut(ctx, req, body)); err != nil {
					fmt.Fprintf(os.Stderr, "cacheprog: write response for put %d: %v\n", req.ID, err)
				}
			}(req, body)

		case "close":
			cancel()
			return r.writeResponse(progResponse{ID: req.ID})

		default:
			_ = r.writeResponse(progResponse{ID: req.ID, Err: "unknown command: " + req.Command})
		}
	}
}

// actionHex returns the hex encoding of an actionID for use as a filename and URL segment.
func (r *cacheprogRunner) actionHex(actionID []byte) string {
	return hex.EncodeToString(actionID)
}

func (r *cacheprogRunner) handleGet(ctx context.Context, req progRequest) progResponse {
	actionHex := r.actionHex(req.ActionID)
	localFile := filepath.Join(r.localDir, actionHex)
	metaFile := localFile + ".meta"

	// Fast path: check local disk before hitting the network.
	if data, err := os.ReadFile(metaFile); err == nil {
		var meta localMeta
		if err := json.Unmarshal(data, &meta); err == nil {
			if outputIDBytes, err := hex.DecodeString(meta.OutputID); err == nil {
				// Verify the blob file still exists before returning DiskPath.
				if _, err := os.Stat(localFile); err == nil {
					return progResponse{
						ID:       req.ID,
						OutputID: outputIDBytes,
						Size:     meta.Size,
						Time:     meta.Time,
						DiskPath: localFile,
					}
				}
			}
		}
	}

	// Fetch from content-cache server.
	url := r.serverURL + "/buildcache/" + actionHex
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cacheprog: creating get request %s: %v\n", actionHex, err)
		return progResponse{ID: req.ID, Miss: true}
	}
	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cacheprog: get %s: %v\n", actionHex, err)
		return progResponse{ID: req.ID, Miss: true}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return progResponse{ID: req.ID, Miss: true}
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "cacheprog: get %s: unexpected status %d\n", actionHex, resp.StatusCode)
		return progResponse{ID: req.ID, Miss: true}
	}

	outputIDHex := resp.Header.Get("X-Output-ID")
	outputIDBytes, err := hex.DecodeString(outputIDHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cacheprog: get %s: invalid X-Output-ID header: %v\n", actionHex, err)
		return progResponse{ID: req.ID, Miss: true}
	}

	// Write blob to a temp file then rename atomically.
	f, err := os.CreateTemp(r.localDir, "tmp-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "cacheprog: creating temp file: %v\n", err)
		return progResponse{ID: req.ID, Miss: true}
	}
	tmpPath := f.Name()

	size, err := io.Copy(f, resp.Body)
	_ = f.Close()
	if err != nil {
		_ = os.Remove(tmpPath)
		fmt.Fprintf(os.Stderr, "cacheprog: writing %s: %v\n", actionHex, err)
		return progResponse{ID: req.ID, Miss: true}
	}

	if err := os.Rename(tmpPath, localFile); err != nil {
		_ = os.Remove(tmpPath)
		fmt.Fprintf(os.Stderr, "cacheprog: renaming %s: %v\n", actionHex, err)
		return progResponse{ID: req.ID, Miss: true}
	}

	fi, _ := os.Stat(localFile)
	var modTime *time.Time
	if fi != nil {
		t := fi.ModTime()
		modTime = &t
	}

	writeSidecar(metaFile, localMeta{OutputID: outputIDHex, Size: size, Time: modTime})

	return progResponse{
		ID:       req.ID,
		OutputID: outputIDBytes,
		Size:     size,
		Time:     modTime,
		DiskPath: localFile,
	}
}

func (r *cacheprogRunner) handlePut(ctx context.Context, req progRequest, body []byte) progResponse {
	actionHex := r.actionHex(req.ActionID)
	outputIDHex := hex.EncodeToString(req.OutputID)
	localFile := filepath.Join(r.localDir, actionHex)
	metaFile := localFile + ".meta"

	// Write body to a temp file first so we can both upload it and rename it.
	f, err := os.CreateTemp(r.localDir, "tmp-*")
	if err != nil {
		return progResponse{ID: req.ID, Err: fmt.Sprintf("creating temp file: %v", err)}
	}
	tmpPath := f.Name()

	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return progResponse{ID: req.ID, Err: fmt.Sprintf("writing body: %v", err)}
	}
	_ = f.Close()

	// Upload to content-cache server.
	uploadURL := r.serverURL + "/buildcache/" + actionHex + "?output_id=" + outputIDHex
	uploadReq, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, bytes.NewReader(body))
	if err != nil {
		_ = os.Remove(tmpPath)
		return progResponse{ID: req.ID, Err: fmt.Sprintf("creating upload request: %v", err)}
	}
	uploadReq.ContentLength = int64(len(body))

	uploadResp, err := r.httpClient.Do(uploadReq)
	if err != nil {
		_ = os.Remove(tmpPath)
		return progResponse{ID: req.ID, Err: fmt.Sprintf("uploading blob: %v", err)}
	}
	_, _ = io.Copy(io.Discard, uploadResp.Body)
	_ = uploadResp.Body.Close()

	if uploadResp.StatusCode != http.StatusNoContent {
		_ = os.Remove(tmpPath)
		return progResponse{ID: req.ID, Err: fmt.Sprintf("upload returned status %d", uploadResp.StatusCode)}
	}

	// Move temp file to its final local path.
	if err := os.Rename(tmpPath, localFile); err != nil {
		_ = os.Remove(tmpPath)
		return progResponse{ID: req.ID, Err: fmt.Sprintf("renaming blob: %v", err)}
	}

	now := time.Now()
	writeSidecar(metaFile, localMeta{OutputID: outputIDHex, Size: int64(len(body)), Time: &now})

	return progResponse{ID: req.ID, DiskPath: localFile}
}

// writeSidecar stores metadata alongside the cached blob file.
// Errors are non-fatal; the sidecar just won't be used for the fast path next time.
func writeSidecar(path string, meta localMeta) {
	data, err := json.Marshal(meta)
	if err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0o600)
}
