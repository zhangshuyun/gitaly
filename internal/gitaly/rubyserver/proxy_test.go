package rubyserver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/metadata"
)

func TestSetHeadersBlocksUnknownMetadata(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	otherKey := "unknown-key"
	otherValue := "test-value"
	inCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(otherKey, otherValue))

	testRepo, _, cleanup := gittest.CloneRepo(t)
	defer cleanup()

	outCtx, err := SetHeaders(inCtx, testhelper.DefaultLocator(), testRepo)
	require.NoError(t, err)

	outMd, ok := metadata.FromOutgoingContext(outCtx)
	require.True(t, ok, "outgoing context should have metadata")

	_, ok = outMd[otherKey]
	require.False(t, ok, "outgoing MD should not contain non-allowlisted key")
}

func TestSetHeadersPreservesAllowlistedMetadata(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	key := "gitaly-servers"
	value := "test-value"
	inCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(key, value))

	testRepo, _, cleanup := gittest.CloneRepo(t)
	defer cleanup()

	outCtx, err := SetHeaders(inCtx, testhelper.DefaultLocator(), testRepo)
	require.NoError(t, err)

	outMd, ok := metadata.FromOutgoingContext(outCtx)
	require.True(t, ok, "outgoing context should have metadata")

	require.Equal(t, []string{value}, outMd[key], "outgoing MD should contain allowlisted key")
}

func TestRubyFeatureHeaders(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	key := "gitaly-feature-ruby-test-feature"
	value := "true"
	inCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(key, value))

	testRepo, _, cleanup := gittest.CloneRepo(t)
	defer cleanup()

	outCtx, err := SetHeaders(inCtx, testhelper.DefaultLocator(), testRepo)
	require.NoError(t, err)

	outMd, ok := metadata.FromOutgoingContext(outCtx)
	require.True(t, ok, "outgoing context should have metadata")

	require.Equal(t, []string{value}, outMd[key], "outgoing MD should contain allowlisted feature key")
}
