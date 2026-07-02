package compress_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/compress"
)

func TestLZ4(t *testing.T) {
	t.Parallel()

	runCompressorSuite(t, compress.NewLZ4())
}

func TestLZ4Encoding(t *testing.T) {
	t.Parallel()

	require.Equal(t, "application/x-lz4", compress.NewLZ4().Encoding())
}
