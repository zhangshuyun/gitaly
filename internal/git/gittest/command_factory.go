package gittest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// NewCommandFactory creates a new Git command factory.
func NewCommandFactory(tb testing.TB, cfg config.Cfg, opts ...git.ExecCommandFactoryOption) git.CommandFactory {
	tb.Helper()
	factory, cleanup, err := git.NewExecCommandFactory(cfg, opts...)
	require.NoError(tb, err)
	tb.Cleanup(cleanup)
	return factory
}
