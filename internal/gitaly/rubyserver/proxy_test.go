package rubyserver

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"google.golang.org/grpc/metadata"
)

func TestSetHeadersBlocksUnknownMetadata(t *testing.T) {
	cfg, repo, _, cleanup := testcfg.BuildWithRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	otherKey := "unknown-key"
	otherValue := "test-value"
	inCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(otherKey, otherValue))

	outCtx, err := SetHeaders(inCtx, config.NewLocator(cfg), repo)
	require.NoError(t, err)

	outMd, ok := metadata.FromOutgoingContext(outCtx)
	require.True(t, ok, "outgoing context should have metadata")

	_, ok = outMd[otherKey]
	require.False(t, ok, "outgoing MD should not contain non-allowlisted key")
}

func TestSetHeadersPreservesAllowlistedMetadata(t *testing.T) {
	cfg, repo, _, cleanup := testcfg.BuildWithRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	key := "gitaly-servers"
	value := "test-value"
	inCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(key, value))

	outCtx, err := SetHeaders(inCtx, config.NewLocator(cfg), repo)
	require.NoError(t, err)

	outMd, ok := metadata.FromOutgoingContext(outCtx)
	require.True(t, ok, "outgoing context should have metadata")

	require.Equal(t, []string{value}, outMd[key], "outgoing MD should contain allowlisted key")
}

func TestRubyFeatureHeaders(t *testing.T) {
	cfg, repo, _, cleanup := testcfg.BuildWithRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	key := "gitaly-feature-ruby-test-feature"
	value := "true"
	inCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(key, value))

	outCtx, err := SetHeaders(inCtx, config.NewLocator(cfg), repo)
	require.NoError(t, err)

	outMd, ok := metadata.FromOutgoingContext(outCtx)
	require.True(t, ok, "outgoing context should have metadata")

	require.Equal(t, []string{value}, outMd[key], "outgoing MD should contain allowlisted feature key")
}
