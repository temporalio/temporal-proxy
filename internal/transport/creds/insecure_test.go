package creds_test

import (
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

func TestInsecure_Validate(t *testing.T) {
	t.Parallel()

	// Insecure has no certificate material to inspect; Validate is a no-op.
	require.NoError(t, creds.NewInsecure().Validate())
}
