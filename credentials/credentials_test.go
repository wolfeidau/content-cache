package credentials

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveReader_EnvFunction(t *testing.T) {
	t.Setenv("TEST_TOKEN", "secret123")

	input := `{"auth_token": {{ env "TEST_TOKEN" | json }}}`
	r := NewResolver()
	creds, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, "secret123", creds.AuthToken)
}

func TestResolveReader_EnvFunctionMissing(t *testing.T) {
	input := `{"auth_token": {{ env "NONEXISTENT_VAR_XYZ" | json }}}`
	r := NewResolver()
	_, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.Error(t, err)
	require.Contains(t, err.Error(), "NONEXISTENT_VAR_XYZ")
}

func TestResolveReader_EnvDefaultFunction(t *testing.T) {
	input := `{"auth_token": {{ envDefault "NONEXISTENT_VAR_XYZ" "fallback" | json }}}`
	r := NewResolver()
	creds, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, "fallback", creds.AuthToken)
}

func TestResolveReader_EnvDefaultWithSetVar(t *testing.T) {
	t.Setenv("TEST_VAR", "actual")

	input := `{"auth_token": {{ envDefault "TEST_VAR" "fallback" | json }}}`
	r := NewResolver()
	creds, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, "actual", creds.AuthToken)
}

func TestResolveReader_FileFunction(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "token.txt")
	err := os.WriteFile(tmpFile, []byte("file-secret\n"), 0o600)
	require.NoError(t, err)

	input := `{"auth_token": {{ file "` + tmpFile + `" | json }}}`
	r := NewResolver()
	creds, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, "file-secret", creds.AuthToken)
}

func TestResolveReader_JSONEscaping(t *testing.T) {
	t.Setenv("TEST_SPECIAL", `value with "quotes" and \backslash`)

	input := `{"auth_token": {{ env "TEST_SPECIAL" | json }}}`
	r := NewResolver()
	creds, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, `value with "quotes" and \backslash`, creds.AuthToken)
}

func TestResolveReader_MockProvider(t *testing.T) {
	callCount := 0
	mockProvider := func(_ context.Context, ref string) (string, error) {
		callCount++
		return "resolved-" + ref, nil
	}

	input := `{"auth_token": {{ mock "my-secret" | json }}}`
	r := NewResolver(WithProvider("mock", mockProvider))
	creds, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, "resolved-my-secret", creds.AuthToken)
	require.Equal(t, 1, callCount)
}

func TestResolveReader_ProviderMemoization(t *testing.T) {
	callCount := 0
	mockProvider := func(_ context.Context, ref string) (string, error) {
		callCount++
		return "resolved-" + ref, nil
	}

	// Same provider+ref used twice
	input := `{
		"auth_token": {{ mock "same-ref" | json }},
		"npm": {"routes": [{"match": {"any": true}, "registry_url": "https://registry.npmjs.org", "token": {{ mock "same-ref" | json }}}]}
	}`
	r := NewResolver(WithProvider("mock", mockProvider))
	creds, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.NoError(t, err)
	require.Equal(t, "resolved-same-ref", creds.AuthToken)
	require.Equal(t, 1, callCount, "provider should only be called once due to memoization")
}

func TestResolveReader_FullCredentials(t *testing.T) {
	t.Setenv("NPM_TOKEN", "npm-secret")
	t.Setenv("GIT_PAT", "git-secret")
	t.Setenv("GHCR_USER", "ghcr-user")
	t.Setenv("GHCR_PASS", "ghcr-pass")

	input := `{
		"auth_token": "inbound-token",
		"npm": {
			"routes": [
				{
					"match": {"scope": "@mycompany"},
					"registry_url": "https://npm.pkg.github.com",
					"token": {{ env "NPM_TOKEN" | json }}
				},
				{
					"match": {"any": true},
					"registry_url": "https://registry.npmjs.org"
				}
			]
		},
		"git": {
			"routes": [
				{
					"match": {"repo_prefix": "github.com/orgA/"},
					"username": "x-access-token",
					"password": {{ env "GIT_PAT" | json }}
				},
				{
					"match": {"any": true}
				}
			]
		},
		"oci": {
			"registries": [
				{
					"prefix": "docker-hub",
					"upstream": "https://registry-1.docker.io"
				},
				{
					"prefix": "ghcr",
					"upstream": "https://ghcr.io",
					"username": {{ env "GHCR_USER" | json }},
					"password": {{ env "GHCR_PASS" | json }}
				}
			]
		}
	}`

	r := NewResolver()
	creds, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.NoError(t, err)

	require.Equal(t, "inbound-token", creds.AuthToken)

	require.NotNil(t, creds.NPM)
	require.Len(t, creds.NPM.Routes, 2)
	require.Equal(t, "@mycompany", creds.NPM.Routes[0].Match.Scope)
	require.Equal(t, "npm-secret", creds.NPM.Routes[0].Token)
	require.True(t, creds.NPM.Routes[1].Match.Any)

	require.NotNil(t, creds.Git)
	require.Len(t, creds.Git.Routes, 2)
	require.Equal(t, "github.com/orgA/", creds.Git.Routes[0].Match.RepoPrefix)
	require.Equal(t, "git-secret", creds.Git.Routes[0].Password)

	require.NotNil(t, creds.OCI)
	require.Len(t, creds.OCI.Registries, 2)
	require.Equal(t, "ghcr-user", creds.OCI.Registries[1].Username)
	require.Equal(t, "ghcr-pass", creds.OCI.Registries[1].Password)
}

func TestResolveReader_MissingKeyError(t *testing.T) {
	input := `{"auth_token": {{ .UndefinedKey }}}`
	r := NewResolver()
	_, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.Error(t, err)
	require.Contains(t, err.Error(), "executing credentials template")
}

func TestResolveReader_InvalidJSON(t *testing.T) {
	input := `not valid json`
	r := NewResolver()
	_, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid credentials JSON after template execution")
}

func TestResolveReader_EmptyInput(t *testing.T) {
	input := `{}`
	r := NewResolver()
	creds, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.NoError(t, err)
	require.Empty(t, creds.AuthToken)
	require.Nil(t, creds.NPM)
	require.Nil(t, creds.OCI)
	require.Nil(t, creds.Git)
}

func TestResolveFile(t *testing.T) {
	t.Setenv("TEST_TOKEN", "from-file")

	tmpFile := filepath.Join(t.TempDir(), "creds.json.tmpl")
	err := os.WriteFile(tmpFile, []byte(`{"auth_token": {{ env "TEST_TOKEN" | json }}}`), 0o600)
	require.NoError(t, err)

	r := NewResolver()
	creds, err := r.ResolveFile(context.Background(), tmpFile)
	require.NoError(t, err)
	require.Equal(t, "from-file", creds.AuthToken)
}

func TestResolveFile_NotFound(t *testing.T) {
	r := NewResolver()
	_, err := r.ResolveFile(context.Background(), "/nonexistent/path")
	require.Error(t, err)
	require.Contains(t, err.Error(), "opening credentials file")
}

func TestResolveReader_OversizedInput(t *testing.T) {
	// Create input larger than maxInputSize
	input := strings.Repeat("x", maxInputSize+1)
	r := NewResolver()
	_, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds maximum size")
}

func TestResolveReader_PartialCredentials(t *testing.T) {
	input := `{
		"git": {
			"routes": [
				{
					"match": {"repo_prefix": "github.com/org/"},
					"username": "user",
					"password": "pass"
				},
				{
					"match": {"any": true}
				}
			]
		}
	}`

	r := NewResolver()
	creds, err := r.ResolveReader(context.Background(), strings.NewReader(input))
	require.NoError(t, err)
	require.Nil(t, creds.NPM)
	require.Nil(t, creds.OCI)
	require.NotNil(t, creds.Git)
	require.Len(t, creds.Git.Routes, 2)
}
