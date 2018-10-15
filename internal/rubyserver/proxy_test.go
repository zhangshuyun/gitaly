package rubyserver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/metadata"
)

func TestSetHeadersBlocksUnknownMetadata(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	otherKey := "unknown-key"
	otherValue := "test-value"
	inCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(otherKey, otherValue))

	outCtx, err := SetHeaders(inCtx, testRepo)
	require.NoError(t, err)

	outMd, ok := metadata.FromOutgoingContext(outCtx)
	require.True(t, ok, "outgoing context should have metadata")

	_, ok = outMd[otherKey]
	require.False(t, ok, "outgoing MD should not contain non-whitelisted key")
}

func TestSetHeadersPreservesWhitelistedMetadata(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	key := "gitaly-servers"
	require.Contains(t, ProxyHeaderWhitelist, key, "sanity check")
	value := "test-value"
	inCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(key, value))

	outCtx, err := SetHeaders(inCtx, testRepo)
	require.NoError(t, err)

	outMd, ok := metadata.FromOutgoingContext(outCtx)
	require.True(t, ok, "outgoing context should have metadata")

	require.Equal(t, []string{value}, outMd[key], "outgoing MD should contain whitelisted key")
}
