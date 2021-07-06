package repository

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRenameRepositorySuccess(t *testing.T) {
	t.Parallel()
	cfg, repo, _, client := setupRepositoryService(t)

	req := &gitalypb.RenameRepositoryRequest{Repository: repo, RelativePath: "a-new-location"}

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RenameRepository(ctx, req)
	require.NoError(t, err)

	newDirectory := filepath.Join(cfg.Storages[0].Path, req.RelativePath)
	require.DirExists(t, newDirectory)
	defer func() { require.NoError(t, os.RemoveAll(newDirectory)) }()

	require.True(t, storage.IsGitDirectory(newDirectory), "moved Git repository has been corrupted")

	// ensure the git directory that got renamed contains a sha in the seed repo
	gittest.GitObjectMustExist(t, cfg.Git.BinPath, newDirectory, "913c66a37b4a45b9769037c55c2d238bd0942d2e")
}

func TestRenameRepositoryDestinationExists(t *testing.T) {
	t.Parallel()
	cfg, repo, _, client := setupRepositoryService(t)

	destinationRepo, destinationRepoPath, cleanupDestinationRepo := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "dst")
	t.Cleanup(cleanupDestinationRepo)

	sha := gittest.WriteCommit(t, cfg, destinationRepoPath)

	req := &gitalypb.RenameRepositoryRequest{Repository: repo, RelativePath: destinationRepo.GetRelativePath()}

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RenameRepository(ctx, req)
	testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)

	// ensure the git directory that already existed didn't get overwritten
	gittest.GitObjectMustExist(t, cfg.Git.BinPath, destinationRepoPath, sha.String())
}

func TestRenameRepositoryInvalidRequest(t *testing.T) {
	t.Parallel()
	_, repo, _, client := setupRepositoryService(t)

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
			req:  &gitalypb.RenameRepositoryRequest{Repository: repo, RelativePath: ""},
		},
		{
			desc: "destination relative path contains path traversal",
			req:  &gitalypb.RenameRepositoryRequest{Repository: repo, RelativePath: "../usr/bin"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.RenameRepository(ctx, tc.req)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}
