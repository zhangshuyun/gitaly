package localrepo_test

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestRepo_FetchInternal(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, remoteRepoProto, _ := testcfg.BuildWithRepo(t)
	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
		))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(
			deps.GetCfg(),
			deps.GetHookManager(),
			deps.GetGitCmdFactory(),
			deps.GetPackObjectsCache(),
		))
	})

	remoteRepo := localrepo.NewTestRepo(t, cfg, remoteRepoProto)
	testhelper.BuildGitalySSH(t, cfg)
	testhelper.BuildGitalyHooks(t, cfg)

	remoteOID, err := remoteRepo.ResolveRevision(ctx, git.Revision("refs/heads/master"))
	require.NoError(t, err)

	tagV100OID, err := remoteRepo.ResolveRevision(ctx, git.Revision("refs/tags/v1.0.0"))
	require.NoError(t, err)

	tagV110OID, err := remoteRepo.ResolveRevision(ctx, git.Revision("refs/tags/v1.1.0"))
	require.NoError(t, err)

	t.Run("refspec with tag", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		require.NoError(t, repo.Config().Set(ctx, "fetch.writeCommitGraph", "true"))

		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master:refs/heads/master"},
			localrepo.FetchOpts{},
		))

		fetchHead := testhelper.MustReadFile(t, filepath.Join(repoPath, "FETCH_HEAD"))
		expectedFetchHead := fmt.Sprintf("%s\t\tbranch 'master' of ssh://gitaly/internal\n", remoteOID.String())
		expectedFetchHead += fmt.Sprintf("%s\tnot-for-merge\ttag 'v1.0.0' of ssh://gitaly/internal\n", tagV100OID.String())
		expectedFetchHead += fmt.Sprintf("%s\tnot-for-merge\ttag 'v1.1.0' of ssh://gitaly/internal", tagV110OID.String())
		require.Equal(t, expectedFetchHead, text.ChompBytes(fetchHead))

		oid, err := repo.ResolveRevision(ctx, git.Revision("refs/heads/master"))
		require.NoError(t, err, "the object from remote should exists in local after fetch done")
		require.Equal(t, remoteOID, oid)

		// Even if the gitconfig says we should write a commit graph, Gitaly should refuse
		// to do so.
		require.NoFileExists(t, filepath.Join(repoPath, "objects/info/commit-graph"))
		require.NoDirExists(t, filepath.Join(repoPath, "objects/info/commit-graphs"))
	})

	t.Run("refspec without tags", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		require.NoError(t, repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master:refs/heads/master"},
			localrepo.FetchOpts{Tags: localrepo.FetchOptsTagsNone},
		))

		fetchHead := testhelper.MustReadFile(t, filepath.Join(repoPath, "FETCH_HEAD"))
		expectedFetchHead := fmt.Sprintf("%s\t\tbranch 'master' of ssh://gitaly/internal", remoteOID.String())
		require.Equal(t, expectedFetchHead, text.ChompBytes(fetchHead))

		oid, err := repo.ResolveRevision(ctx, git.Revision("refs/heads/master"))
		require.NoError(t, err, "the object from remote should exists in local after fetch done")
		require.Equal(t, remoteOID, oid)
	})

	t.Run("object ID", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
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
		ctx := testhelper.MergeIncomingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		var stderr bytes.Buffer
		err := repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/does/not/exist"},
			localrepo.FetchOpts{Stderr: &stderr},
		)
		require.EqualError(t, err, "exit status 128")
		require.IsType(t, err, localrepo.ErrFetchFailed{})
		require.Equal(t, "fatal: couldn't find remote ref refs/does/not/exist\nfatal: the remote end hung up unexpectedly\n", stderr.String())
	})

	t.Run("with env", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		var stderr bytes.Buffer
		err := repo.FetchInternal(
			ctx, remoteRepoProto, []string{"refs/heads/master"},
			localrepo.FetchOpts{Stderr: &stderr, Env: []string{"GIT_TRACE=1"}},
		)
		require.NoError(t, err)
		require.Contains(t, stderr.String(), "trace: built-in: git fetch")
	})

	t.Run("invalid remote repo", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
		repo := localrepo.NewTestRepo(t, cfg, repoProto)

		err := repo.FetchInternal(ctx, &gitalypb.Repository{
			RelativePath: "does/not/exist",
			StorageName:  cfg.Storages[0].Name,
		}, []string{"refs/does/not/exist"}, localrepo.FetchOpts{})
		require.Error(t, err)
		require.IsType(t, err, localrepo.ErrFetchFailed{})
		require.Contains(t, err.Error(), "GetRepoPath: not a git repository")
	})

	t.Run("pruning", func(t *testing.T) {
		ctx := testhelper.MergeIncomingMetadata(ctx, testhelper.GitalyServersMetadataFromCfg(t, cfg))

		repoProto, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
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
