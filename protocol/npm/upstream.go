package npm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	// DefaultRegistryURL is the default NPM registry.
	DefaultRegistryURL = "https://registry.npmjs.org"

	// DefaultTimeout is the default timeout for upstream requests.
	DefaultTimeout = 30 * time.Second
)

// ErrNotFound is returned when a package or version is not found.
var ErrNotFound = errors.New("not found")

// Upstream fetches packages from an upstream NPM registry.
type Upstream struct {
	baseURL string
	client  *http.Client
}

// UpstreamOption configures an Upstream.
type UpstreamOption func(*Upstream)

// WithRegistryURL sets the upstream registry URL.
func WithRegistryURL(url string) UpstreamOption {
	return func(u *Upstream) {
		u.baseURL = strings.TrimSuffix(url, "/")
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) UpstreamOption {
	return func(u *Upstream) {
		u.client = client
	}
}

// NewUpstream creates a new upstream registry client.
func NewUpstream(opts ...UpstreamOption) *Upstream {
	u := &Upstream{
		baseURL: DefaultRegistryURL,
		client: &http.Client{
			Timeout: DefaultTimeout,
		},
	}
	for _, opt := range opts {
		opt(u)
	}
	return u
}

// FetchPackageMetadata fetches full metadata for a package.
func (u *Upstream) FetchPackageMetadata(ctx context.Context, name string) (*PackageMetadata, error) {
	url := fmt.Sprintf("%s/%s", u.baseURL, encodePackageName(name))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	// Request full metadata
	req.Header.Set("Accept", "application/json")

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upstream returned %d: %s", resp.StatusCode, string(body))
	}

	var meta PackageMetadata
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decoding metadata: %w", err)
	}

	return &meta, nil
}

// FetchPackageMetadataRaw fetches raw metadata JSON for a package.
func (u *Upstream) FetchPackageMetadataRaw(ctx context.Context, name string) ([]byte, error) {
	url := fmt.Sprintf("%s/%s", u.baseURL, encodePackageName(name))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upstream returned %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// FetchAbbreviatedMetadata fetches abbreviated metadata for a package.
// This is faster and uses less bandwidth than full metadata.
func (u *Upstream) FetchAbbreviatedMetadata(ctx context.Context, name string) (*AbbreviatedMetadata, error) {
	url := fmt.Sprintf("%s/%s", u.baseURL, encodePackageName(name))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	// Request abbreviated metadata
	req.Header.Set("Accept", "application/vnd.npm.install-v1+json")

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upstream returned %d: %s", resp.StatusCode, string(body))
	}

	var meta AbbreviatedMetadata
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		return nil, fmt.Errorf("decoding metadata: %w", err)
	}

	return &meta, nil
}

// FetchTarball fetches a package tarball.
// Returns a ReadCloser that must be closed by the caller.
func (u *Upstream) FetchTarball(ctx context.Context, tarballURL string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tarballURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("performing request: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
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

// FetchTarballByName fetches a tarball by package name and version.
func (u *Upstream) FetchTarballByName(ctx context.Context, name, version string) (io.ReadCloser, error) {
	tarballURL := u.TarballURL(name, version)
	return u.FetchTarball(ctx, tarballURL)
}

// TarballURL returns the URL for a package tarball.
func (u *Upstream) TarballURL(name, version string) string {
	// Handle scoped packages
	if strings.HasPrefix(name, "@") {
		// @scope/package -> @scope/package/-/package-version.tgz
		parts := strings.SplitN(name, "/", 2)
		if len(parts) == 2 {
			return fmt.Sprintf("%s/%s/-/%s-%s.tgz", u.baseURL, encodePackageName(name), parts[1], version)
		}
	}
	return fmt.Sprintf("%s/%s/-/%s-%s.tgz", u.baseURL, name, name, version)
}

// encodePackageName encodes a package name for use in URLs.
// Scoped packages need special handling: @scope/package -> @scope%2fpackage
func encodePackageName(name string) string {
	if strings.HasPrefix(name, "@") {
		// URL-encode the slash in scoped packages
		return strings.Replace(name, "/", "%2f", 1)
	}
	return url.PathEscape(name)
}

// decodePackageName decodes a URL-encoded package name.
func decodePackageName(encoded string) (string, error) {
	decoded, err := url.PathUnescape(encoded)
	if err != nil {
		return "", err
	}
	return decoded, nil
}
