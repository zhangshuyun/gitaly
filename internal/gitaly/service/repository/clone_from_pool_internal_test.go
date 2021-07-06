package repository

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

func newTestObjectPool(t *testing.T, cfg config.Cfg) (*objectpool.ObjectPool, *gitalypb.Repository) {
	t.Helper()
	relativePath := gittest.NewObjectPoolName(t)
	repo := gittest.InitRepoDir(t, cfg.Storages[0].Path, relativePath)

	gitCmdFactory := git.NewExecCommandFactory(cfg)

	pool, err := objectpool.NewObjectPool(
		cfg,
		config.NewLocator(cfg),
		gitCmdFactory,
		catfile.NewCache(cfg),
		repo.GetStorageName(),
		relativePath,
	)
	require.NoError(t, err)

	return pool, repo
}

// getForkDestination creates a repo struct and path, but does not actually create the directory
func getForkDestination(t *testing.T, storage config.Storage) (*gitalypb.Repository, string, func()) {
	t.Helper()
	folder, err := text.RandomHex(20)
	require.NoError(t, err)
	forkRepoPath := filepath.Join(storage.Path, folder)
	forkedRepo := &gitalypb.Repository{StorageName: storage.Name, RelativePath: folder, GlRepository: "project-1"}

	return forkedRepo, forkRepoPath, func() { os.RemoveAll(forkRepoPath) }
}

func TestCloneFromPoolInternal(t *testing.T) {
	t.Parallel()
	cfg, repo, repoPath, client := setupRepositoryService(t)

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

	req := &gitalypb.CloneFromPoolInternalRequest{
		Repository:       forkedRepo,
		SourceRepository: repo,
		Pool: &gitalypb.ObjectPool{
			Repository: poolRepo,
		},
	}

	_, err := client.CloneFromPoolInternal(ctx, req)
	require.NoError(t, err)

	assert.True(t, gittest.GetGitObjectDirSize(t, forkRepoPath) < 100)

	isLinked, err := pool.LinkedToRepository(repo)
	require.NoError(t, err)
	require.True(t, isLinked)

	// feature is a branch known to exist in the source repository. By looking it up in the target
	// we establish that the target has branches, even though (as we saw above) it has no objects.
	gittest.Exec(t, cfg, "-C", forkRepoPath, "show-ref", "--verify", "refs/heads/feature")
	gittest.Exec(t, cfg, "-C", forkRepoPath, "show-ref", "--verify", "refs/heads/branch")
}

// fullRepack does a full repack on the repository, which means if it has a pool repository linked, it will get rid of redundant objects that are reachable in the pool
func fullRepack(t *testing.T, cfg config.Cfg, repoPath string) {
	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-l", "-d")
}
