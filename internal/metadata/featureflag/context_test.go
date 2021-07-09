package featureflag

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

var mockFeatureFlag = FeatureFlag{"turn meow on", false}

func TestIncomingCtxWithFeatureFlag(t *testing.T) {
	ctx := context.Background()
	require.False(t, mockFeatureFlag.IsEnabled(ctx))

	ctx = IncomingCtxWithFeatureFlag(ctx, mockFeatureFlag)
	require.True(t, mockFeatureFlag.IsEnabled(ctx))
}

func TestIncomingCtxWithDisabledFeatureFlag(t *testing.T) {
	ctx := context.Background()

	require.False(t, mockFeatureFlag.IsEnabled(ctx))

	ctx = IncomingCtxWithDisabledFeatureFlag(ctx, mockFeatureFlag)

	require.True(t, mockFeatureFlag.IsDisabled(ctx))
}

func TestOutgoingCtxWithFeatureFlag(t *testing.T) {
	ctx := context.Background()
	require.False(t, mockFeatureFlag.IsEnabled(ctx))

	ctx = OutgoingCtxWithFeatureFlags(ctx, mockFeatureFlag)
	require.False(t, mockFeatureFlag.IsEnabled(ctx))

	// simulate an outgoing context leaving the process boundary and then
	// becoming an incoming context in a new process boundary
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	ctx = metadata.NewIncomingContext(context.Background(), md)
	require.True(t, mockFeatureFlag.IsEnabled(ctx))
}
