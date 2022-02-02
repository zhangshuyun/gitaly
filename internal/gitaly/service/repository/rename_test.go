package repository

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRenameRepository_success(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	// Praefect does not move repositories on the disk so this test case is not run with Praefect.
	cfg, repo, _, client := setupRepositoryService(ctx, t, testserver.WithDisablePraefect())

	const targetPath = "a-new-location"
	_, err := client.RenameRepository(ctx, &gitalypb.RenameRepositoryRequest{
		Repository:   repo,
		RelativePath: targetPath,
	})
	require.NoError(t, err)

	newDirectory := filepath.Join(cfg.Storages[0].Path, targetPath)
	require.DirExists(t, newDirectory)
	defer func() { require.NoError(t, os.RemoveAll(newDirectory)) }()

	require.True(t, storage.IsGitDirectory(newDirectory), "moved Git repository has been corrupted")

	// ensure the git directory that got renamed contains a sha in the seed repo
	gittest.RequireObjectExists(t, cfg, newDirectory, git.ObjectID("913c66a37b4a45b9769037c55c2d238bd0942d2e"))
}

func TestRenameRepository_DestinationExists(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg, client := setupRepositoryServiceWithoutRepo(t)

	existingDestinationRepo := &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "repository-1"}
	_, err := client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: existingDestinationRepo})
	require.NoError(t, err)

	renamedRepo := &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "repository-2"}
	_, err = client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: renamedRepo})
	require.NoError(t, err)

	destinationRepoPath := filepath.Join(cfg.Storages[0].Path, getReplicaPath(ctx, t, client, existingDestinationRepo))
	commitID := gittest.WriteCommit(t, cfg, destinationRepoPath, gittest.WithParents())

	_, err = client.RenameRepository(ctx, &gitalypb.RenameRepositoryRequest{
		Repository:   renamedRepo,
		RelativePath: existingDestinationRepo.RelativePath,
	})
	testhelper.RequireGrpcCode(t, err, codes.AlreadyExists)

	// ensure the git directory that already existed didn't get overwritten
	gittest.RequireObjectExists(t, cfg, destinationRepoPath, commitID)
}

func TestRenameRepository_invalidRequest(t *testing.T) {
	// Prafect applies renames to metadata even on failed requests, which fails this test.
	testhelper.SkipWithPraefect(t, "https://gitlab.com/gitlab-org/gitaly/-/issues/4003")

	t.Parallel()
	ctx := testhelper.Context(t)

	_, repo, _, client := setupRepositoryService(ctx, t)

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
			testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
		})
	}
}
