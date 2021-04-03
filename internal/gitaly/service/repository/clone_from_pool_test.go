package repository_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

func TestCloneFromPoolHTTP(t *testing.T) {
	serverSocketPath := runFullServer(t)

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	pool, poolRepo := NewTestObjectPool(t)
	defer pool.Remove(ctx)

	require.NoError(t, pool.Create(ctx, testRepo))
	require.NoError(t, pool.Link(ctx, testRepo))

	fullRepack(t, testRepoPath)

	_, newBranch := gittest.CreateCommitOnNewBranch(t, testRepoPath)

	forkedRepo, forkRepoPath, forkRepoCleanup := getForkDestination(t)
	defer forkRepoCleanup()

	authorizationHeader := "ABCefg0999182"
	_, remoteURL := gittest.RemoteUploadPackServer(ctx, t, config.Config.Git.BinPath, "my-repo", authorizationHeader, testRepoPath)

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

	isLinked, err := pool.LinkedToRepository(testRepo)
	require.NoError(t, err)
	require.True(t, isLinked, "repository is not linked to the pool repository")

	assert.True(t, gittest.GetGitObjectDirSize(t, forkRepoPath) < 100, "expect a small object directory size")

	// feature is a branch known to exist in the source repository. By looking it up in the target
	// we establish that the target has branches, even though (as we saw above) it has no objects.
	testhelper.MustRunCommand(t, nil, "git", "-C", forkRepoPath, "show-ref", "--verify", "refs/heads/feature")
	testhelper.MustRunCommand(t, nil, "git", "-C", forkRepoPath, "show-ref", "--verify", fmt.Sprintf("refs/heads/%s", newBranch))
}
