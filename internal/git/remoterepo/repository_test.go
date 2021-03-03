package remoterepo_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestRepository(t *testing.T) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder()
	defer cfgBuilder.Cleanup()
	cfg := cfgBuilder.Build(t)

	serverSocketPath, cleanup := testserver.RunGitalyServer(t, cfg, nil)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, err := helper.InjectGitalyServers(ctx, "default", serverSocketPath, cfg.Auth.Token)
	require.NoError(t, err)

	pool := client.NewPool()
	defer pool.Close()

	gittest.TestRepository(t, cfg, func(t testing.TB, pbRepo *gitalypb.Repository) git.Repository {
		t.Helper()

		r, err := remoterepo.New(helper.OutgoingToIncoming(ctx), pbRepo, pool)
		require.NoError(t, err)
		return r
	})
}
