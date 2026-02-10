package opprovider

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wolfeidau/content-cache/credentials"
)

func TestWithOnePassword_RegistersProvider(t *testing.T) {
	// We can only verify the option doesn't panic during construction.
	// Actually calling `op read` requires the 1Password CLI installed.
	opt := WithOnePassword()
	r := credentials.NewResolver(opt)
	require.NotNil(t, r)
}
