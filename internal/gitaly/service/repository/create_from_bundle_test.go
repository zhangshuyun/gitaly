package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServer_CreateRepositoryFromBundle_successful(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	locator := config.NewLocator(cfg)
	tmpdir, err := tempdir.New(ctx, repo, locator)
	require.NoError(t, err)
	bundlePath := filepath.Join(tmpdir, "original.bundle")

	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/custom-refs/ref1", "HEAD")

	gittest.Exec(t, cfg, "-C", repoPath, "bundle", "create", bundlePath, "--all")
	defer os.RemoveAll(bundlePath)

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	importedRepoProto := &gitalypb.Repository{
		StorageName:  repo.GetStorageName(),
		RelativePath: "a-repo-from-bundle",
	}
	importedRepo := localrepo.NewTestRepo(t, cfg, importedRepoProto)
	importedRepoPath, err := locator.GetPath(importedRepoProto)
	require.NoError(t, err)
	defer os.RemoveAll(importedRepoPath)

	request := &gitalypb.CreateRepositoryFromBundleRequest{Repository: importedRepoProto}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p

		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.CreateRepositoryFromBundleRequest{}

		return nil
	})

	file, err := os.Open(bundlePath)
	require.NoError(t, err)
	defer file.Close()

	_, err = io.Copy(writer, file)
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	gittest.Exec(t, cfg, "-C", importedRepoPath, "fsck")

	info, err := os.Lstat(filepath.Join(importedRepoPath, "hooks"))
	require.NoError(t, err)
	require.NotEqual(t, 0, info.Mode()&os.ModeSymlink)

	commit, err := importedRepo.ReadCommit(ctx, "refs/custom-refs/ref1")
	require.NoError(t, err)
	require.NotNil(t, commit)
}

func TestServer_CreateRepositoryFromBundle_transactional(t *testing.T) {
	var votes []voting.Vote
	txManager := &transaction.MockManager{
		VoteFn: func(ctx context.Context, tx txinfo.Transaction, vote voting.Vote) error {
			votes = append(votes, vote)
			return nil
		},
	}

	cfg, repoProto, repoPath, client := setupRepositoryService(t,
		testserver.WithTransactionManager(txManager))

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx, err := txinfo.InjectTransaction(ctx, 1, "primary", true)
	require.NoError(t, err)
	ctx = helper.IncomingToOutgoing(ctx)

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{
		Repository: &gitalypb.Repository{
			StorageName:  repoProto.GetStorageName(),
			RelativePath: "create.git",
		},
	}))

	bundle := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "create", "-", "master", "feature")
	require.Greater(t, len(bundle), 100*1024)

	_, err = io.Copy(streamio.NewWriter(func(p []byte) error {
		require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{
			Data: p,
		}))
		return nil
	}), bytes.NewReader(bundle))
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	masterOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "master"))
	featureOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "feature"))

	// This accounts for the first two votes which first do a git-clone(1) followed by a fetch.
	// Given that voting is done via git's reference-transaction hook, the format is `<oldrev>
	// <newrev> <reference>`.
	fetchInput := fmt.Sprintf("%s %s refs/heads/feature\n%s %s refs/heads/master\n",
		git.ZeroOID, featureOID, git.ZeroOID, masterOID)

	// And this accounts for the final vote in `Create()`, which does vote on all references in
	// the target repo as listed by git-for-each-ref(1).
	refsInput := fmt.Sprintf("%s commit\trefs/heads/feature\n%s commit\trefs/heads/master\n",
		featureOID, masterOID)

	require.Equal(t, []voting.Vote{
		voting.VoteFromData([]byte(fetchInput)),
		voting.VoteFromData([]byte(fetchInput)),
		voting.VoteFromData([]byte(refsInput)),
	}, votes)
}

func TestServer_CreateRepositoryFromBundle_failed_invalid_bundle(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	importedRepo := &gitalypb.Repository{
		StorageName:  cfg.Storages[0].Name,
		RelativePath: "a-repo-from-bundle",
	}
	importedRepoPath := filepath.Join(cfg.Storages[0].Path, importedRepo.GetRelativePath())
	defer os.RemoveAll(importedRepoPath)

	request := &gitalypb.CreateRepositoryFromBundleRequest{Repository: importedRepo}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p

		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.CreateRepositoryFromBundleRequest{}

		return nil
	})

	_, err = io.Copy(writer, bytes.NewBufferString("not-a-bundle"))
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid gitfile format")
}

func TestServer_CreateRepositoryFromBundle_failed_validations(t *testing.T) {
	_, client := setupRepositoryServiceWithoutRepo(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{}))

	_, err = stream.CloseAndRecv()
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestServer_CreateRepositoryFromBundle_failed_existing_directory(t *testing.T) {
	_, repo, _, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{
		Repository: repo,
	}))

	_, err = stream.CloseAndRecv()
	testassert.GrpcEqualErr(t, status.Error(codes.FailedPrecondition, "CreateRepositoryFromBundle: target directory is non-empty"), err)
}

func TestSanitizedError(t *testing.T) {
	testCases := []struct {
		path     string
		format   string
		a        []interface{}
		expected string
	}{
		{
			path:     "/home/git/storage",
			format:   "failed to create from bundle in /home/git/storage/my-project",
			expected: "failed to create from bundle in [REPO PATH]/my-project",
		},
		{
			path:     "/home/git/storage",
			format:   "failed to %s in [REPO PATH]/my-project",
			a:        []interface{}{"create from bundle"},
			expected: "failed to create from bundle in [REPO PATH]/my-project",
		},
	}

	for _, tc := range testCases {
		str := sanitizedError(tc.path, tc.format, tc.a...)
		assert.Equal(t, tc.expected, str)
	}
}
