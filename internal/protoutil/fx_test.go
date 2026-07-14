package protoutil_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/fx"

	"github.com/temporalio/temporal-proxy/internal/protoutil"
)

func TestModule(t *testing.T) {
	t.Parallel()

	start := mustMarshal(t, &workflowservice.StartWorkflowExecutionRequest{Namespace: "ns-1"})

	t.Run("provides an extractor backed by the global registries", func(t *testing.T) {
		t.Parallel()

		var ex *protoutil.Extractor
		app := fx.New(
			protoutil.Module,
			fx.Populate(&ex),
			fx.NopLogger,
		)
		require.NoError(t, app.Err())
		require.NotNil(t, ex)
		require.Equal(t, "ns-1", ex.Namespace(startMethod, start))
	})

	t.Run("supplied Files overrides the default", func(t *testing.T) {
		t.Parallel()

		var ex *protoutil.Extractor
		app := fx.New(
			fx.Supply(fx.Annotate(errFiles{err: errors.New("boom")}, fx.As(new(protoutil.Files)))),
			protoutil.Module,
			fx.Populate(&ex),
			fx.NopLogger,
		)
		require.NoError(t, app.Err())
		require.NotNil(t, ex)
		require.Equal(t, "", ex.Namespace(startMethod, start))
	})
}
