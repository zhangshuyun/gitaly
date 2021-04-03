package remoterepo_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestRepository(t *testing.T) {
	cfg := testcfg.Build(t)

	serverSocketPath := testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(deps.GetCfg(), deps.GetRubyServer(), deps.GetLocator(), deps.GetTxManager(), deps.GetGitCmdFactory()))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(deps.GetCfg(), deps.GetLocator(), deps.GetGitCmdFactory(), deps.GetLinguist()))
	})

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
