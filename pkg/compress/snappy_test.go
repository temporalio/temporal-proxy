package compress_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/compress"
)

func TestSnappy(t *testing.T) {
	t.Parallel()

	runCompressorSuite(t, compress.NewSnappy())
}

func TestSnappyEncoding(t *testing.T) {
	t.Parallel()

	require.Equal(t, "application/x-snappy-framed", compress.NewSnappy().Encoding())
}
