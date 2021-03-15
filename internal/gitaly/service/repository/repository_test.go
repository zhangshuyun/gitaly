package repository

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRepositoryExists(t *testing.T) {
	storageOtherDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	// Setup storage paths
	testStorages := []config.Storage{
		{Name: "default", Path: testhelper.GitlabTestStoragePath()},
		{Name: "other", Path: storageOtherDir},
		{Name: "broken", Path: "/does/not/exist"},
	}

	defer func(oldStorages []config.Storage) {
		config.Config.Storages = oldStorages
	}(config.Config.Storages)
	config.Config.Storages = testStorages

	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator, testhelper.WithStorages([]string{"default", "other", "broken"}))
	defer stop()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	queries := []struct {
		desc      string
		request   *gitalypb.RepositoryExistsRequest
		errorCode codes.Code
		exists    bool
	}{
		{
			desc: "repository nil",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: nil,
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "storage name empty",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "",
					RelativePath: testRepo.GetRelativePath(),
				},
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "relative path empty",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  testRepo.GetStorageName(),
					RelativePath: "",
				},
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "exists true",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  testRepo.GetStorageName(),
					RelativePath: testRepo.GetRelativePath(),
				},
			},
			exists: true,
		},
		{
			desc: "exists false, wrong storage",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "other",
					RelativePath: testRepo.GetRelativePath(),
				},
			},
			exists: false,
		},
		{
			desc: "storage directory does not exist",
			request: &gitalypb.RepositoryExistsRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "broken",
					RelativePath: "foobar.git",
				},
			},
			errorCode: codes.NotFound,
		},
	}

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()
			response, err := client.RepositoryExists(ctx, tc.request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			if err != nil {
				// Ignore the response message if there was an error
				return
			}

			require.Equal(t, tc.exists, response.Exists)
		})
	}
}

func TestSuccessfulHasLocalBranches(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	emptyRepoName := "empty-repo.git"
	emptyRepoPath := filepath.Join(testhelper.GitlabTestStoragePath(), emptyRepoName)
	testhelper.MustRunCommand(t, nil, "git", "init", "--bare", emptyRepoPath)
	defer os.RemoveAll(emptyRepoPath)

	testCases := []struct {
		desc      string
		request   *gitalypb.HasLocalBranchesRequest
		value     bool
		errorCode codes.Code
	}{
		{
			desc:    "repository has branches",
			request: &gitalypb.HasLocalBranchesRequest{Repository: testRepo},
			value:   true,
		},
		{
			desc: "repository doesn't have branches",
			request: &gitalypb.HasLocalBranchesRequest{
				Repository: &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: emptyRepoName,
				},
			},
			value: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.HasLocalBranches(ctx, tc.request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))
			if err != nil {
				return
			}

			require.Equal(t, tc.value, response.Value)
		})
	}
}

func TestFailedHasLocalBranches(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc       string
		repository *gitalypb.Repository
		errorCode  codes.Code
	}{
		{
			desc:       "repository nil",
			repository: nil,
			errorCode:  codes.InvalidArgument,
		},
		{
			desc:       "repository doesn't exist",
			repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"},
			errorCode:  codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &gitalypb.HasLocalBranchesRequest{Repository: tc.repository}
			_, err := client.HasLocalBranches(ctx, request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))
		})
	}
}
