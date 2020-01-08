package repository

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRenameRepositorySuccess(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tempDir, cleanupTempDir := testhelper.TempDir(t, t.Name())
	defer cleanupTempDir()

	destinationPath := filepath.Join(tempDir, "a", "new", "location")

	req := &gitalypb.RenameRepositoryRequest{Repository: testRepo, RelativePath: destinationPath}

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RenameRepository(ctx, req)
	require.NoError(t, err)

	newDirectory, err := helper.GetPath(&gitalypb.Repository{StorageName: "default", RelativePath: destinationPath})
	require.NoError(t, err)
	require.DirExists(t, newDirectory)

	require.True(t, helper.IsGitDirectory(newDirectory), "moved Git repository has been corrupted")

	// ensure the git directory that got renamed contains a sha in the seed repo
	testhelper.GitObjectMustExist(t, newDirectory, "913c66a37b4a45b9769037c55c2d238bd0942d2e")
}

func TestRenameRepositoryDestinationExists(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	destinationRepo, destinationRepoPath, cleanupDestinationRepo := testhelper.NewTestRepo(t)
	defer cleanupDestinationRepo()

	_, sha := testhelper.CreateCommitOnNewBranch(t, destinationRepoPath)

	req := &gitalypb.RenameRepositoryRequest{Repository: testRepo, RelativePath: destinationRepo.GetRelativePath()}

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RenameRepository(ctx, req)
	testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)

	// ensure the git directory that already existed didn't get overwritten
	testhelper.GitObjectMustExist(t, destinationRepoPath, sha)
}

func TestRenameRepositoryInvalidRequest(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
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
