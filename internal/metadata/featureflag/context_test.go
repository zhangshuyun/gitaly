package featureflag

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

var mockFeatureFlag = GoFeatureFlag{"turn meow on", false}

func TestIncomingCtxWithFeatureFlag(t *testing.T) {
	ctx := context.Background()
	require.False(t, IsEnabled(ctx, mockFeatureFlag))

	ctx = IncomingCtxWithFeatureFlag(ctx, mockFeatureFlag)
	require.True(t, IsEnabled(ctx, mockFeatureFlag))
}

func TestOutgoingCtxWithFeatureFlag(t *testing.T) {
	ctx := context.Background()
	require.False(t, IsEnabled(ctx, mockFeatureFlag))

	ctx = OutgoingCtxWithFeatureFlags(ctx, mockFeatureFlag)
	require.False(t, IsEnabled(ctx, mockFeatureFlag))

	// simulate an outgoing context leaving the process boundary and then
	// becoming an incoming context in a new process boundary
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)

	ctx = metadata.NewIncomingContext(context.Background(), md)
	require.True(t, IsEnabled(ctx, mockFeatureFlag))
}
