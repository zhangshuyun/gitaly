package helper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

//nolint:forbidigo // We're explicitly testing that deadline cancellation works as expected.
func TestSuppressCancellation(t *testing.T) {
	type key struct{}

	parentDeadline := time.Now()
	parentCtx, cancel := context.WithDeadline(context.WithValue(context.Background(), key{}, "value"), parentDeadline)
	cancel()

	t.Run("no deadline on suppressed context", func(t *testing.T) {
		ctx := SuppressCancellation(parentCtx)

		deadline, ok := ctx.Deadline()
		require.False(t, ok)
		require.Equal(t, time.Time{}, deadline)

		require.Nil(t, ctx.Done())
		require.NoError(t, ctx.Err())

		require.Equal(t, ctx.Value(key{}), "value")
	})

	t.Run("with deadline on suppressed context", func(t *testing.T) {
		newDeadline := parentDeadline.Add(24 * time.Hour)
		ctx, cancel := context.WithDeadline(SuppressCancellation(parentCtx), newDeadline)

		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Equal(t, newDeadline, deadline)

		require.NoError(t, ctx.Err())
		select {
		case <-ctx.Done():
			t.Fatal("context should not be done yet")
		default:
			require.NotNil(t, ctx.Done())
		}

		require.Equal(t, ctx.Value(key{}), "value")

		cancel()

		require.Error(t, context.Canceled)
		select {
		case <-ctx.Done():
		default:
			t.Fatal("context should have been done")
		}
	})
}
