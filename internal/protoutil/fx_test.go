package protoutil_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/workflowservice/v1"
	"go.uber.org/fx"
	"google.golang.org/protobuf/reflect/protoreflect"

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

	t.Run("provides a translator warmed for the supplied services", func(t *testing.T) {
		t.Parallel()

		var tr *protoutil.Translator
		app := fx.New(
			fx.Supply([]protoreflect.FullName{wfService}),
			protoutil.Module,
			fx.Populate(&tr),
			fx.NopLogger,
		)
		require.NoError(t, app.Err())
		require.NotNil(t, tr)
		require.True(t, tr.IsWarm("temporal.api.workflowservice.v1.StartWorkflowExecutionRequest"))
	})

	t.Run("warming an unknown service fails startup", func(t *testing.T) {
		t.Parallel()

		app := fx.New(
			fx.Supply([]protoreflect.FullName{"does.not.Exist"}),
			protoutil.Module,
			fx.NopLogger,
		)
		require.Error(t, app.Err())
	})
}
