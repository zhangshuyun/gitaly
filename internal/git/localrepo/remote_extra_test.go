package localrepo_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestRepo_FetchInternal(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	gitCmdFactory, readGitProtocol := gittest.NewProtocolDetectingCommandFactory(ctx, t, cfg)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
		))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(
			deps.GetHookManager(),
			deps.GetGitCmdFactory(),
			deps.GetPackObjectsCache(),
		))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
			deps.GetHousekeepingManager(),
		))
	}, testserver.WithGitCommandFactory(gitCmdFactory))

	remoteRepoProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	remoteRepo := localrepo.NewTestRepo(t, cfg, remoteRepoProto)
	testcfg.BuildGitalySSH(t, cfg)
	testcfg.BuildGitalyHooks(t, cfg)

	remoteOID, err := remoteRepo.ResolveRevision(ctx, git.Revision("refs/heads/master"))
	require.NoError(t, err)

	tagV100OID, err := remoteRepo.ResolveRevision(ctx, git.Revision("refs/tags/v1.0.0"))
	require.NoError(t, err)

	tagV110OID, err := remoteRepo.ResolveRevision(ctx, git.Revision("refs/tags/v1.1.0"))
	require.NoError(t, err)

	t.Run("refspec with tag", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		gittest.Exec(t, cfg, "-C", repoPath, "config", "fetch.writeCommitGraph", "true")

		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master:refs/heads/master"},
			localrepo.FetchOpts{},
		))

		refs, err := repo.GetReferences(ctx)
		require.NoError(t, err)
		require.Equal(t, []git.Reference{
			{Name: "refs/heads/master", Target: remoteOID.String()},
			{Name: "refs/tags/v1.0.0", Target: tagV100OID.String()},
			{Name: "refs/tags/v1.1.0", Target: tagV110OID.String()},
		}, refs)

		// Even if the gitconfig says we should write a commit graph, Gitaly should refuse
		// to do so.
		require.NoFileExists(t, filepath.Join(repoPath, "objects/info/commit-graph"))
		require.NoDirExists(t, filepath.Join(repoPath, "objects/info/commit-graphs"))

		// Assert that we're using the expected Git protocol version, which is protocol v2.
		require.Equal(t, "GIT_PROTOCOL=version=2\n", readGitProtocol())

		require.NoFileExists(t, filepath.Join(repoPath, "FETCH_HEAD"))
	})

	t.Run("refspec without tags", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(ctx, t, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master:refs/heads/master"},
			localrepo.FetchOpts{Tags: localrepo.FetchOptsTagsNone},
		))

		refs, err := repo.GetReferences(ctx)
		require.NoError(t, err)
		require.Equal(t, []git.Reference{
			{Name: "refs/heads/master", Target: remoteOID.String()},
		}, refs)
	})

	t.Run("object ID", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(ctx, t, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{remoteOID.String()},
			localrepo.FetchOpts{},
		))

		exists, err := repo.HasRevision(ctx, remoteOID.Revision()+"^{commit}")
		require.NoError(t, err, "the object from remote should exists in local after fetch done")
		require.True(t, exists)
	})

	t.Run("nonexistent revision", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(ctx, t, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		var stderr bytes.Buffer
		err := repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/does/not/exist"},
			localrepo.FetchOpts{Stderr: &stderr},
		)
		require.EqualError(t, err, "exit status 128")
		require.IsType(t, err, localrepo.ErrFetchFailed{})
		require.Equal(t, stderr.String(), "fatal: couldn't find remote ref refs/does/not/exist\n")
	})

	t.Run("with env", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(ctx, t, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		var stderr bytes.Buffer
		err := repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master"},
			localrepo.FetchOpts{Stderr: &stderr, Env: []string{"GIT_TRACE=1"}},
		)
		require.NoError(t, err)
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --no-write-fetch-head --quiet --atomic --end-of-options")
	})

	t.Run("with disabled transactions", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(ctx, t, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		var stderr bytes.Buffer
		err := repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master"},
			localrepo.FetchOpts{Stderr: &stderr, Env: []string{"GIT_TRACE=1"}, DisableTransactions: true},
		)
		require.NoError(t, err)
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --no-write-fetch-head --quiet --end-of-options")
	})

	t.Run("invalid remote repo", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(ctx, t, cfg)
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		err := repo.FetchInternal(ctx, &gitalypb.Repository{
			RelativePath: "does/not/exist",
			StorageName:  cfg.Storages[0].Name,
		}, []string{"refs/does/not/exist"}, localrepo.FetchOpts{})
		require.Error(t, err)
		require.IsType(t, err, localrepo.ErrFetchFailed{})

		expectedMsg := "GetRepoPath: not a git repository"
		if testhelper.IsPraefectEnabled() {
			expectedMsg = `repository \"default\"/\"does/not/exist\" not found`
		}

		require.Contains(t, err.Error(), expectedMsg)
	})

	t.Run("pruning", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testcfg.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
			Seed: gittest.SeedGitLabTest,
		})
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		// Create a local reference. Given that it doesn't exist on the remote side, it
		// would get pruned if we pass `--prune`.
		require.NoError(t, repo.UpdateRef(ctx, "refs/heads/prune-me", remoteOID, git.ZeroOID))

		// By default, refs are not pruned.
		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/*:refs/*"}, localrepo.FetchOpts{},
		))

		exists, err := repo.HasRevision(ctx, "refs/heads/prune-me")
		require.NoError(t, err)
		require.True(t, exists)

		// But they are pruned if we pass the `WithPrune()` option.
		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/*:refs/*"}, localrepo.FetchOpts{Prune: true},
		))

		exists, err = repo.HasRevision(ctx, "refs/heads/prune-me")
		require.NoError(t, err)
		require.False(t, exists)
	})
}
