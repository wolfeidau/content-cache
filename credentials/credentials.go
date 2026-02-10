package credentials

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"text/template"
)

const (
	// maxInputSize is the maximum size of a credentials template file (1MB).
	maxInputSize = 1 << 20
	// maxOutputSize is the maximum size of rendered template output (1MB).
	maxOutputSize = 1 << 20
)

// Credentials holds all resolved credential values.
type Credentials struct {
	AuthToken string         `json:"auth_token,omitempty"`
	NPM       *NPMAuthConfig `json:"npm,omitempty"`
	OCI       *OCIAuthConfig `json:"oci,omitempty"`
	Git       *GitAuthConfig `json:"git,omitempty"`
}

// NPMAuthConfig holds NPM routing table configuration.
type NPMAuthConfig struct {
	Routes []NPMRoute `json:"routes,omitempty"`
}

// NPMRoute defines a single NPM routing rule.
type NPMRoute struct {
	Match       NPMRouteMatch `json:"match"`
	RegistryURL string        `json:"registry_url"`
	Token       string        `json:"token,omitempty"`
}

// NPMRouteMatch defines the matching criteria for an NPM route.
type NPMRouteMatch struct {
	Scope string `json:"scope,omitempty"`
	Any   bool   `json:"any,omitempty"`
}

// OCIAuthConfig holds OCI registry configuration.
type OCIAuthConfig struct {
	Registries []OCIRegistry `json:"registries,omitempty"`
}

// OCIRegistry defines a single OCI registry entry.
type OCIRegistry struct {
	Prefix   string `json:"prefix"`
	Upstream string `json:"upstream"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	TagTTL   string `json:"tag_ttl,omitempty"`
}

// GitAuthConfig holds Git routing table configuration.
type GitAuthConfig struct {
	Routes []GitRoute `json:"routes,omitempty"`
}

// GitRoute defines a single Git routing rule.
type GitRoute struct {
	Match    GitRouteMatch `json:"match"`
	Username string        `json:"username,omitempty"`
	Password string        `json:"password,omitempty"`
}

// GitRouteMatch defines the matching criteria for a Git route.
type GitRouteMatch struct {
	RepoPrefix string `json:"repo_prefix,omitempty"`
	Any        bool   `json:"any,omitempty"`
}

// SecretProvider resolves a secret reference to its value.
type SecretProvider func(ctx context.Context, ref string) (string, error)

// ResolverOption configures a Resolver.
type ResolverOption func(*Resolver)

// Resolver executes a template file and parses the result into Credentials.
type Resolver struct {
	providers map[string]SecretProvider
	logger    *slog.Logger
}

// WithLogger sets the logger for the resolver.
func WithLogger(logger *slog.Logger) ResolverOption {
	return func(r *Resolver) {
		r.logger = logger
	}
}

// WithProvider registers a named secret provider as a template function.
func WithProvider(name string, p SecretProvider) ResolverOption {
	return func(r *Resolver) {
		r.providers[name] = p
	}
}

// NewResolver creates a new credential resolver with the given options.
func NewResolver(opts ...ResolverOption) *Resolver {
	r := &Resolver{
		providers: make(map[string]SecretProvider),
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// ResolveFile reads and resolves a credentials template file.
func (r *Resolver) ResolveFile(ctx context.Context, path string) (*Credentials, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening credentials file: %w", err)
	}
	defer f.Close()

	return r.ResolveReader(ctx, f)
}

// ResolveReader resolves a credentials template from a reader.
func (r *Resolver) ResolveReader(ctx context.Context, reader io.Reader) (*Credentials, error) {
	limited := io.LimitReader(reader, maxInputSize+1)

	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("reading credentials template: %w", err)
	}

	if len(data) > maxInputSize {
		return nil, fmt.Errorf("credentials template exceeds maximum size of %d bytes", maxInputSize)
	}

	// Build template function map with memoization cache.
	cache := make(map[string]string)
	funcMap := r.buildFuncMap(ctx, cache)

	tmpl, err := template.New("credentials").
		Option("missingkey=error").
		Funcs(funcMap).
		Parse(string(data))
	if err != nil {
		return nil, fmt.Errorf("parsing credentials template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, nil); err != nil {
		return nil, fmt.Errorf("executing credentials template: %w", err)
	}

	if buf.Len() > maxOutputSize {
		return nil, fmt.Errorf("rendered credentials exceed maximum size of %d bytes", maxOutputSize)
	}

	var creds Credentials
	if err := json.Unmarshal(buf.Bytes(), &creds); err != nil {
		return nil, fmt.Errorf("invalid credentials JSON after template execution: %w", err)
	}

	return &creds, nil
}

// buildFuncMap creates the template function map with built-in and provider functions.
func (r *Resolver) buildFuncMap(ctx context.Context, cache map[string]string) template.FuncMap {
	fm := template.FuncMap{
		"env": func(key string) (string, error) {
			val, ok := os.LookupEnv(key)
			if !ok {
				return "", fmt.Errorf("environment variable %q is not set", key)
			}
			return val, nil
		},
		"envDefault": func(key, fallback string) string {
			if val, ok := os.LookupEnv(key); ok {
				return val
			}
			return fallback
		},
		"file": func(path string) (string, error) {
			data, err := os.ReadFile(path)
			if err != nil {
				return "", fmt.Errorf("reading file %q: %w", path, err)
			}
			return strings.TrimSpace(string(data)), nil
		},
		"json": func(v string) (string, error) {
			b, err := json.Marshal(v)
			if err != nil {
				return "", fmt.Errorf("JSON encoding value: %w", err)
			}
			return string(b), nil
		},
	}

	// Register provider functions.
	for name, provider := range r.providers {
		fm[name] = r.makeProviderFunc(ctx, name, provider, cache)
	}

	return fm
}

// makeProviderFunc creates a memoized template function for a secret provider.
func (r *Resolver) makeProviderFunc(ctx context.Context, name string, provider SecretProvider, cache map[string]string) func(string) (string, error) {
	return func(ref string) (string, error) {
		cacheKey := name + ":" + ref
		if val, ok := cache[cacheKey]; ok {
			return val, nil
		}

		val, err := provider(ctx, ref)
		if err != nil {
			return "", fmt.Errorf("provider %q failed for ref %q: %w", name, ref, err)
		}

		cache[cacheKey] = val
		return val, nil
	}
}
