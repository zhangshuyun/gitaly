package hook

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamcache"
)

func isNullCache(c streamcache.Cache) bool {
	_, ok := c.(streamcache.NullCache)
	return ok
}

func TestNewServer(t *testing.T) {
	testCases := []struct {
		desc      string
		cfg       config.Cfg
		nullCache bool
	}{
		{
			desc:      "cache disabled",
			nullCache: true,
		},
		{
			desc: "cache enabled",
			cfg: config.Cfg{
				PackObjectsCache: config.PackObjectsCache{
					Enabled: true,
				},
			},
			nullCache: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := tc.cfg
			poc := NewServer(
				cfg,
				hook.NewManager(config.NewLocator(cfg), transaction.NewManager(cfg, backchannel.NewRegistry()), gitlab.NewMockClient(), cfg),
				git.NewExecCommandFactory(cfg),
			).(*server).packObjectsCache

			require.NotNil(t, poc)
			require.Equal(t, tc.nullCache, isNullCache(poc))
		})
	}
}
