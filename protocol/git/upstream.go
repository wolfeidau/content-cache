package git

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
)

// Upstream fetches from upstream Git repositories over HTTPS.
type Upstream struct {
	client   *http.Client
	logger   *slog.Logger
	username string
	password string
}

// UpstreamOption configures an Upstream.
type UpstreamOption func(*Upstream)

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) UpstreamOption {
	return func(u *Upstream) {
		u.client = client
	}
}

// WithUpstreamLogger sets the logger for the upstream client.
func WithUpstreamLogger(logger *slog.Logger) UpstreamOption {
	return func(u *Upstream) {
		u.logger = logger
	}
}

// WithBasicAuth sets the username and password for upstream authentication.
// This covers GitHub PATs (username=x-access-token, password=<PAT>),
// GitLab tokens, and Bitbucket app passwords.
func WithBasicAuth(username, password string) UpstreamOption {
	return func(u *Upstream) {
		u.username = username
		u.password = password
	}
}

// setAuth sets Basic Auth on the request if credentials are configured.
// Auth is applied if username is set (password may be empty, which is valid for some providers).
func (u *Upstream) setAuth(req *http.Request) {
	if u.username != "" {
		req.SetBasicAuth(u.username, u.password)
	}
}

// NewUpstream creates a new upstream Git client.
// The default HTTP client uses no Client.Timeout — it relies on context
// deadlines instead, since large repo clones can take minutes.
func NewUpstream(opts ...UpstreamOption) *Upstream {
	u := &Upstream{
		client: &http.Client{},
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(u)
	}
	return u
}

// FetchInfoRefs fetches the info/refs discovery response from the upstream repository.
// The gitProtocol parameter is forwarded as the Git-Protocol header if non-empty.
func (u *Upstream) FetchInfoRefs(ctx context.Context, repo RepoRef, gitProtocol string) (io.ReadCloser, string, error) {
	url := fmt.Sprintf("%s/info/refs?service=git-upload-pack", repo.UpstreamURL())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("creating info/refs request: %w", err)
	}

	if gitProtocol != "" {
		req.Header.Set("Git-Protocol", gitProtocol)
	}
	u.setAuth(req)

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("fetching info/refs: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, "", ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, "", fmt.Errorf("upstream info/refs returned %d: %s", resp.StatusCode, string(body))
	}

	return resp.Body, resp.Header.Get("Content-Type"), nil
}

// FetchUploadPack sends a git-upload-pack request to the upstream repository.
// The gitProtocol parameter is forwarded as the Git-Protocol header if non-empty.
func (u *Upstream) FetchUploadPack(ctx context.Context, repo RepoRef, gitProtocol string, body io.Reader) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/git-upload-pack", repo.UpstreamURL())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, fmt.Errorf("creating upload-pack request: %w", err)
	}

	req.Header.Set("Content-Type", ContentTypeUploadPackRequest)
	if gitProtocol != "" {
		req.Header.Set("Git-Protocol", gitProtocol)
	}
	u.setAuth(req)

	u.logger.Debug("sending upload-pack request to upstream",
		"url", url,
		"git_protocol", gitProtocol,
		"content_type", req.Header.Get("Content-Type"),
	)

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching upload-pack: %w", err)
	}

	u.logger.Debug("upstream upload-pack response",
		"url", url,
		"status", resp.StatusCode,
		"content_type", resp.Header.Get("Content-Type"),
		"content_length", resp.ContentLength,
	)

	if resp.StatusCode == http.StatusNotFound {
		_ = resp.Body.Close()
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()

		u.logger.Error("upstream upload-pack error response",
			"url", url,
			"status", resp.StatusCode,
			"content_type", resp.Header.Get("Content-Type"),
			"content_length", resp.Header.Get("Content-Length"),
			"response_body_length", len(respBody),
			"response_body", string(respBody),
		)

		return nil, fmt.Errorf("upstream upload-pack returned %d: %s", resp.StatusCode, string(respBody))
	}

	return resp.Body, nil
}
