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
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/praefectutil"
	"gitlab.com/gitlab-org/gitaly/v14/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateRepositoryFromBundle_successful(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testServerCreateRepositoryFromBundleSuccessful)
}

func testServerCreateRepositoryFromBundleSuccessful(t *testing.T, ctx context.Context) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	locator := config.NewLocator(cfg)
	tmpdir, err := tempdir.New(ctx, repo.GetStorageName(), locator)
	require.NoError(t, err)
	bundlePath := filepath.Join(tmpdir.Path(), "original.bundle")

	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/custom-refs/ref1", "HEAD")

	gittest.Exec(t, cfg, "-C", repoPath, "bundle", "create", bundlePath, "--all")
	defer func() { require.NoError(t, os.RemoveAll(bundlePath)) }()

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	importedRepoProto := &gitalypb.Repository{
		StorageName:  repo.GetStorageName(),
		RelativePath: "a-repo-from-bundle",
	}

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

	importedRepoProto.RelativePath = getReplicaPath(ctx, t, client, importedRepoProto)
	importedRepo := localrepo.NewTestRepo(t, cfg, importedRepoProto)
	importedRepoPath, err := locator.GetPath(importedRepoProto)
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(importedRepoPath)) }()

	gittest.Exec(t, cfg, "-C", importedRepoPath, "fsck")

	_, err = os.Lstat(filepath.Join(importedRepoPath, "hooks"))
	require.True(t, os.IsNotExist(err), "hooks directory should not have been created")

	commit, err := importedRepo.ReadCommit(ctx, "refs/custom-refs/ref1")
	require.NoError(t, err)
	require.NotNil(t, commit)
}

func TestCreateRepositoryFromBundle_transactional(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testCreateRepositoryFromBundleTransactional)
}

func testCreateRepositoryFromBundleTransactional(t *testing.T, ctx context.Context) {
	var votes []voting.Vote
	txManager := &transaction.MockManager{
		VoteFn: func(ctx context.Context, tx txinfo.Transaction, vote voting.Vote) error {
			votes = append(votes, vote)
			return nil
		},
	}

	cfg, repoProto, repoPath, client := setupRepositoryService(t,
		testserver.WithTransactionManager(txManager))

	masterOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/master"))
	featureOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/feature"))

	// keep-around refs are not cloned in the initial step, but are added via the second call to
	// git-fetch(1). We thus create some of them to exercise their behaviour with regards to
	// transactional voting.
	for _, keepAroundRef := range []string{"refs/keep-around/1", "refs/keep-around/2"} {
		gittest.Exec(t, cfg, "-C", repoPath, "update-ref", keepAroundRef, masterOID)
	}

	ctx, err := txinfo.InjectTransaction(ctx, 1, "primary", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{
		Repository: &gitalypb.Repository{
			StorageName:  repoProto.GetStorageName(),
			RelativePath: "create.git",
		},
	}))

	bundle := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "create", "-",
		"refs/heads/master", "refs/heads/feature", "refs/keep-around/1", "refs/keep-around/2")
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

	if featureflag.TxAtomicRepositoryCreation.IsEnabled(ctx) {
		var actualVotes []string
		for _, vote := range votes {
			actualVotes = append(actualVotes, vote.String())
		}

		// While the following votes are opaque to us, this doesn't really matter. All we do
		// care about is that they're stable.
		require.Equal(t, []string{
			// These are the votes created by git-fetch(1).
			"47553c06f575f757ad56ef3216c59804b72aa4a6",
			"47553c06f575f757ad56ef3216c59804b72aa4a6",
			// And this is the manual votes we compute by walking the repository.
			"da39a3ee5e6b4b0d3255bfef95601890afd80709",
			"da39a3ee5e6b4b0d3255bfef95601890afd80709",
		}, actualVotes)
		return
	}

	var votingInput []string

	// This accounts for the first two votes via git-clone(1). Given that git-clone(1) creates
	// all references in a single reference transaction, the hook is executed twice for all
	// fetched references (once for "prepare", once for "commit").
	votingInput = append(votingInput,
		fmt.Sprintf("%s %s refs/heads/feature\n%s %s refs/heads/master\n", git.ZeroOID, featureOID, git.ZeroOID, masterOID),
		fmt.Sprintf("%s %s refs/heads/feature\n%s %s refs/heads/master\n", git.ZeroOID, featureOID, git.ZeroOID, masterOID),
	)

	// Keep-around references are not fetched via git-clone(1) because non-mirror clones only
	// fetch branches and tags. These additional references are thus obtained via git-fetch(1).
	// Given that we use the `--atomic` flag for git-fetch(1), all reference updates will be in
	// a single transaction and we thus expect exactly two votes (once for "prepare", once for
	// "commit").
	votingInput = append(votingInput,
		fmt.Sprintf("%s %s refs/keep-around/2\n%s %s refs/keep-around/1\n", git.ZeroOID, masterOID, git.ZeroOID, masterOID),
		fmt.Sprintf("%s %s refs/keep-around/2\n%s %s refs/keep-around/1\n", git.ZeroOID, masterOID, git.ZeroOID, masterOID),
	)

	var expectedVotes []voting.Vote
	for _, expectedVote := range votingInput {
		expectedVotes = append(expectedVotes, voting.VoteFromData([]byte(expectedVote)))
	}

	require.Equal(t, votes, expectedVotes)
}

func TestCreateRepositoryFromBundle_invalidBundle(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testCreateRepositoryFromBundleInvalidBundle)
}

func testCreateRepositoryFromBundleInvalidBundle(t *testing.T, ctx context.Context) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	importedRepo := &gitalypb.Repository{
		StorageName:  cfg.Storages[0].Name,
		RelativePath: "a-repo-from-bundle",
	}
	importedRepoPath := filepath.Join(cfg.Storages[0].Path, importedRepo.GetRelativePath())
	defer func() { require.NoError(t, os.RemoveAll(importedRepoPath)) }()

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

func TestCreateRepositoryFromBundle_invalidArgument(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testServerCreateRepositoryFromBundleFailedValidations)
}

func testServerCreateRepositoryFromBundleFailedValidations(t *testing.T, ctx context.Context) {
	_, client := setupRepositoryServiceWithoutRepo(t)

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{}))

	_, err = stream.CloseAndRecv()
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestCreateRepositoryFromBundle_existingRepository(t *testing.T) {
	t.Parallel()

	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testServerCreateRepositoryFromBundleFailedExistingDirectory)
}

func testServerCreateRepositoryFromBundleFailedExistingDirectory(t *testing.T, ctx context.Context) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	// The above test creates the second repository on the server. As this test can run with Praefect in front of it,
	// we'll use the next replica path Praefect will assign in order to ensure this repository creation conflicts even
	// with Praefect in front of it.
	repo, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0], gittest.CloneRepoOpts{
		RelativePath: praefectutil.DeriveReplicaPath(1),
	})

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{
		Repository: repo,
	}))

	_, err = stream.CloseAndRecv()

	if featureflag.TxAtomicRepositoryCreation.IsEnabled(ctx) {
		testhelper.GrpcEqualErr(t, status.Error(codes.AlreadyExists, "creating repository: repository exists already"), err)
	} else {
		testhelper.GrpcEqualErr(t, status.Error(codes.FailedPrecondition, "CreateRepositoryFromBundle: target directory is non-empty"), err)
	}
}

func TestSanitizedError(t *testing.T) {
	t.Parallel()
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
