package oci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ErrUnauthorized indicates authentication is required but failed.
var ErrUnauthorized = errors.New("unauthorized")

// AuthCache caches authentication tokens per scope.
type AuthCache struct {
	mu     sync.RWMutex
	tokens map[string]*cachedToken
}

type cachedToken struct {
	token     string
	expiresAt time.Time
}

// NewAuthCache creates a new authentication token cache.
func NewAuthCache() *AuthCache {
	return &AuthCache{
		tokens: make(map[string]*cachedToken),
	}
}

// GetToken returns a valid token for the scope, or empty string if none cached.
func (ac *AuthCache) GetToken(scope string) string {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	if t, ok := ac.tokens[scope]; ok {
		// Check if token is still valid with 30-second buffer
		if time.Now().Add(30 * time.Second).Before(t.expiresAt) {
			return t.token
		}
	}
	return ""
}

// SetToken caches a token for the scope with expiration.
func (ac *AuthCache) SetToken(scope, token string, expiresIn int) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	expiresAt := time.Now().Add(time.Duration(expiresIn) * time.Second)
	ac.tokens[scope] = &cachedToken{
		token:     token,
		expiresAt: expiresAt,
	}
}

// Clear removes all cached tokens.
func (ac *AuthCache) Clear() {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.tokens = make(map[string]*cachedToken)
}

// ParseWWWAuthenticate parses the WWW-Authenticate header from a 401 response.
// Example: Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull"
func ParseWWWAuthenticate(header string) (*AuthChallenge, error) {
	if !strings.HasPrefix(header, "Bearer ") {
		return nil, fmt.Errorf("unsupported auth type: %s", header)
	}

	params := header[7:] // Remove "Bearer " prefix
	challenge := &AuthChallenge{}

	// Parse key="value" pairs
	for _, part := range splitParams(params) {
		key, value, ok := parseParam(part)
		if !ok {
			continue
		}

		switch key {
		case "realm":
			challenge.Realm = value
		case "service":
			challenge.Service = value
		case "scope":
			challenge.Scope = value
		}
	}

	if challenge.Realm == "" {
		return nil, errors.New("missing realm in WWW-Authenticate header")
	}

	return challenge, nil
}

// splitParams splits the parameter string by commas, respecting quoted values.
func splitParams(s string) []string {
	var parts []string
	var current strings.Builder
	inQuotes := false

	for _, r := range s {
		switch r {
		case '"':
			inQuotes = !inQuotes
			current.WriteRune(r)
		case ',':
			if inQuotes {
				current.WriteRune(r)
			} else {
				parts = append(parts, strings.TrimSpace(current.String()))
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}

	return parts
}

// parseParam parses a key="value" parameter.
func parseParam(s string) (key, value string, ok bool) {
	idx := strings.Index(s, "=")
	if idx < 0 {
		return "", "", false
	}

	key = strings.TrimSpace(s[:idx])
	value = strings.TrimSpace(s[idx+1:])

	// Remove surrounding quotes
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		value = value[1 : len(value)-1]
	}

	return key, value, true
}

// FetchToken requests a new token from the auth server.
func FetchToken(ctx context.Context, client *http.Client, challenge *AuthChallenge, username, password string) (*TokenResponse, error) {
	// Build token request URL
	tokenURL, err := url.Parse(challenge.Realm)
	if err != nil {
		return nil, fmt.Errorf("parsing realm URL: %w", err)
	}

	query := tokenURL.Query()
	if challenge.Service != "" {
		query.Set("service", challenge.Service)
	}
	if challenge.Scope != "" {
		query.Set("scope", challenge.Scope)
	}
	tokenURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating token request: %w", err)
	}

	// Add basic auth if credentials provided
	if username != "" && password != "" {
		req.SetBasicAuth(username, password)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return nil, fmt.Errorf("decoding token response: %w", err)
	}

	// Some registries use access_token instead of token
	if tokenResp.Token == "" && tokenResp.AccessToken != "" {
		tokenResp.Token = tokenResp.AccessToken
	}

	// Default expiration if not provided
	if tokenResp.ExpiresIn == 0 {
		tokenResp.ExpiresIn = 300 // 5 minutes default
	}

	return &tokenResp, nil
}

// BuildScope constructs an OCI registry scope string for the given image and action.
func BuildScope(imageName, action string) string {
	return fmt.Sprintf("repository:%s:%s", imageName, action)
}
