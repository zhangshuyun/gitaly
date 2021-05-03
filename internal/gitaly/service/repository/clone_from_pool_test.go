package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

func testCloneFromPoolHTTP(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, repo, repoPath, client := setupRepositoryServiceWithRuby(t, cfg, rubySrv)

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	pool, poolRepo := newTestObjectPool(t, cfg)
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	require.NoError(t, pool.Create(ctx, repo))
	require.NoError(t, pool.Link(ctx, repo))

	fullRepack(t, cfg, repoPath)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("branch"))

	forkedRepo, forkRepoPath, forkRepoCleanup := getForkDestination(t, cfg.Storages[0])
	defer forkRepoCleanup()

	authorizationHeader := "ABCefg0999182"
	_, remoteURL := gittest.RemoteUploadPackServer(ctx, t, cfg.Git.BinPath, "my-repo", authorizationHeader, repoPath)

	req := &gitalypb.CloneFromPoolRequest{
		Repository: forkedRepo,
		Remote: &gitalypb.Remote{
			Url:                     remoteURL,
			HttpAuthorizationHeader: authorizationHeader,
			MirrorRefmaps:           []string{"all_refs"},
		},
		Pool: &gitalypb.ObjectPool{
			Repository: poolRepo,
		},
	}

	_, err := client.CloneFromPool(ctx, req)
	require.NoError(t, err)

	isLinked, err := pool.LinkedToRepository(repo)
	require.NoError(t, err)
	require.True(t, isLinked, "repository is not linked to the pool repository")

	assert.True(t, gittest.GetGitObjectDirSize(t, forkRepoPath) < 100, "expect a small object directory size")

	// feature is a branch known to exist in the source repository. By looking it up in the target
	// we establish that the target has branches, even though (as we saw above) it has no objects.
	gittest.Exec(t, cfg, "-C", forkRepoPath, "show-ref", "--verify", "refs/heads/feature")
	gittest.Exec(t, cfg, "-C", forkRepoPath, "show-ref", "--verify", "refs/heads/branch")
}
