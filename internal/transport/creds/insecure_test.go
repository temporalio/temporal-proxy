package creds_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/internal/transport/creds"
)

func TestInsecure_DialOption(t *testing.T) {
	t.Parallel()

	opt, err := creds.NewInsecure().DialOption()
	require.NoError(t, err)
	require.NotNil(t, opt)
}

func TestInsecure_ServerOption(t *testing.T) {
	t.Parallel()

	opt, err := creds.NewInsecure().ServerOption()
	require.NoError(t, err)
	require.NotNil(t, opt)
}

func writeFile(t *testing.T, dir, name string, contents []byte) string {
	t.Helper()

	path := filepath.Join(dir, name)
	err := os.WriteFile(path, contents, 0o600)
	require.NoError(t, err)

	return path
}
