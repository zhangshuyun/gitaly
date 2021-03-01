package repository

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRenameRepositorySuccess(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	req := &gitalypb.RenameRepositoryRequest{Repository: testRepo, RelativePath: "a-new-location"}

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RenameRepository(ctx, req)
	require.NoError(t, err)

	newDirectory, err := locator.GetPath(&gitalypb.Repository{StorageName: "default", RelativePath: req.RelativePath})
	require.NoError(t, err)
	require.DirExists(t, newDirectory)
	defer func() { require.NoError(t, os.RemoveAll(newDirectory)) }()

	require.True(t, storage.IsGitDirectory(newDirectory), "moved Git repository has been corrupted")

	// ensure the git directory that got renamed contains a sha in the seed repo
	gittest.GitObjectMustExist(t, config.Config.Git.BinPath, newDirectory, "913c66a37b4a45b9769037c55c2d238bd0942d2e")
}

func TestRenameRepositoryDestinationExists(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	destinationRepo, destinationRepoPath, cleanupDestinationRepo := gittest.CloneRepo(t)
	defer cleanupDestinationRepo()

	_, sha := gittest.CreateCommitOnNewBranch(t, destinationRepoPath)

	req := &gitalypb.RenameRepositoryRequest{Repository: testRepo, RelativePath: destinationRepo.GetRelativePath()}

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RenameRepository(ctx, req)
	testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)

	// ensure the git directory that already existed didn't get overwritten
	gittest.GitObjectMustExist(t, config.Config.Git.BinPath, destinationRepoPath, sha)
}

func TestRenameRepositoryInvalidRequest(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc string
		req  *gitalypb.RenameRepositoryRequest
	}{
		{
			desc: "empty repository",
			req:  &gitalypb.RenameRepositoryRequest{Repository: nil, RelativePath: "/tmp/abc"},
		},
		{
			desc: "empty destination relative path",
			req:  &gitalypb.RenameRepositoryRequest{Repository: testRepo, RelativePath: ""},
		},
		{
			desc: "destination relative path contains path traversal",
			req:  &gitalypb.RenameRepositoryRequest{Repository: testRepo, RelativePath: "../usr/bin"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.RenameRepository(ctx, tc.req)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}
