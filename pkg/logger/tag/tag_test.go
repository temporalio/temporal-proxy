package tag_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

func TestString(t *testing.T) {
	t.Parallel()

	tg := tag.String("key", "value")
	require.Equal(t, tag.Tag{Key: "key", Value: "value"}, tg)

	tg = tag.Component("test")
	require.Equal(t, tag.Tag{Key: "component", Value: "test"}, tg)
}

func TestError(t *testing.T) {
	t.Parallel()

	tg := tag.Error(errors.New("boom"))
	require.Equal(t, tag.Tag{Key: "error", Value: "boom"}, tg)

	tg = tag.Error(nil)
	require.Equal(t, tag.Tag{Key: "error", Value: ""}, tg)
}

func TestStringer(t *testing.T) {
	t.Parallel()

	d := 90 * time.Second
	tg := tag.Stringer("dur", d)
	require.Equal(t, tag.Tag{Key: "dur", Value: d.String()}, tg)
}
