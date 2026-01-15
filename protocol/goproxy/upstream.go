package goproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	// DefaultUpstreamURL is the default Go module proxy.
	DefaultUpstreamURL = "https://proxy.golang.org"

	// DefaultTimeout is the default timeout for upstream requests.
	DefaultTimeout = 30 * time.Second
)

// Upstream fetches modules from an upstream Go module proxy.
type Upstream struct {
	baseURL string
	client  *http.Client
}

// UpstreamOption configures an Upstream.
type UpstreamOption func(*Upstream)

// WithUpstreamURL sets the upstream proxy URL.
func WithUpstreamURL(url string) UpstreamOption {
	return func(u *Upstream) {
		u.baseURL = url
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) UpstreamOption {
	return func(u *Upstream) {
		u.client = client
	}
}

// NewUpstream creates a new upstream proxy client.
func NewUpstream(opts ...UpstreamOption) *Upstream {
	u := &Upstream{
		baseURL: DefaultUpstreamURL,
		client: &http.Client{
			Timeout: DefaultTimeout,
		},
	}
	for _, opt := range opts {
		opt(u)
	}
	return u
}

// FetchVersionList fetches the list of versions for a module.
func (u *Upstream) FetchVersionList(ctx context.Context, modulePath string) ([]string, error) {
	url := fmt.Sprintf("%s/%s/@v/list", u.baseURL, encodePath(modulePath))

	body, err := u.fetch(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching version list: %w", err)
	}
	defer func() { _ = body.Close() }()

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading version list: %w", err)
	}

	// Parse newline-separated versions
	content := string(data)
	if content == "" {
		return nil, nil
	}

	var versions []string
	for _, line := range splitLines(content) {
		if line != "" {
			versions = append(versions, line)
		}
	}
	return versions, nil
}

// FetchInfo fetches version info for a module version.
func (u *Upstream) FetchInfo(ctx context.Context, modulePath, version string) (*VersionInfo, error) {
	url := fmt.Sprintf("%s/%s/@v/%s.info", u.baseURL, encodePath(modulePath), version)

	body, err := u.fetch(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching info: %w", err)
	}
	defer func() { _ = body.Close() }()

	var info VersionInfo
	if err := json.NewDecoder(body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decoding info: %w", err)
	}

	return &info, nil
}

// FetchMod fetches the go.mod file for a module version.
func (u *Upstream) FetchMod(ctx context.Context, modulePath, version string) ([]byte, error) {
	url := fmt.Sprintf("%s/%s/@v/%s.mod", u.baseURL, encodePath(modulePath), version)

	body, err := u.fetch(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching mod: %w", err)
	}
	defer func() { _ = body.Close() }()

	data, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading mod: %w", err)
	}

	return data, nil
}

// FetchZip fetches the module zip file.
// Returns a ReadCloser that must be closed by the caller.
func (u *Upstream) FetchZip(ctx context.Context, modulePath, version string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/%s/@v/%s.zip", u.baseURL, encodePath(modulePath), version)

	body, err := u.fetch(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching zip: %w", err)
	}

	return body, nil
}

// FetchLatest fetches the latest version info for a module.
// Note: Not all proxies support this endpoint.
func (u *Upstream) FetchLatest(ctx context.Context, modulePath string) (*VersionInfo, error) {
	url := fmt.Sprintf("%s/%s/@latest", u.baseURL, encodePath(modulePath))

	body, err := u.fetch(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetching latest: %w", err)
	}
	defer func() { _ = body.Close() }()

	var info VersionInfo
	if err := json.NewDecoder(body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decoding latest: %w", err)
	}

	return &info, nil
}

// fetch performs an HTTP GET request and returns the response body.
func (u *Upstream) fetch(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing request: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		_ = resp.Body.Close()
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, fmt.Errorf("upstream returned %d: %s", resp.StatusCode, string(body))
	}

	return resp.Body, nil
}

// splitLines splits a string into lines, handling both \n and \r\n.
func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			line := s[start:i]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			lines = append(lines, line)
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
