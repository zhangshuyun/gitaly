package helper

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func TestRepoPathEqual(t *testing.T) {
	testCases := []struct {
		desc  string
		a, b  *gitalypb.Repository
		equal bool
	}{
		{
			desc: "equal",
			a: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "repo.git",
			},
			b: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "repo.git",
			},
			equal: true,
		},
		{
			desc: "different storage",
			a: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "repo.git",
			},
			b: &gitalypb.Repository{
				StorageName:  "storage2",
				RelativePath: "repo.git",
			},
			equal: false,
		},
		{
			desc: "different path",
			a: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "repo.git",
			},
			b: &gitalypb.Repository{
				StorageName:  "default",
				RelativePath: "repo2.git",
			},
			equal: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			assert.Equal(t, tc.equal, RepoPathEqual(tc.a, tc.b))
		})
	}
}

func TestGetRepoPath(t *testing.T) {
	defer func(oldStorages []config.Storage) {
		config.Config.Storages = oldStorages
	}(config.Config.Storages)

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repoPath, err := GetRepoPath(testRepo)
	if err != nil {
		t.Fatal(err)
	}

	exampleStorages := []config.Storage{
		{Name: "default", Path: testhelper.GitlabTestStoragePath()},
		{Name: "other", Path: "/home/git/repositories2"},
		{Name: "third", Path: "/home/git/repositories3"},
	}

	testCases := []struct {
		desc     string
		storages []config.Storage
		repo     *gitalypb.Repository
		path     string
		err      codes.Code
	}{
		{
			desc:     "storages configured",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: testRepo.GetRelativePath()},
			path:     repoPath,
		},
		{
			desc: "no storage config, storage name provided",
			repo: &gitalypb.Repository{StorageName: "does not exist", RelativePath: testRepo.GetRelativePath()},
			err:  codes.InvalidArgument,
		},
		{
			desc: "no storage config, nil repo",
			err:  codes.InvalidArgument,
		},
		{
			desc:     "storage config provided, empty repo",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{},
			err:      codes.InvalidArgument,
		},
		{
			desc: "no storage config, empty repo",
			repo: &gitalypb.Repository{},
			err:  codes.InvalidArgument,
		},
		{
			desc:     "non existing repo",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "made/up/path.git"},
			err:      codes.NotFound,
		},
		{
			desc:     "non existing storage",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: "does not exists", RelativePath: testRepo.GetRelativePath()},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "storage defined but storage dir does not exist",
			storages: []config.Storage{{Name: testRepo.GetStorageName(), Path: "/does/not/exist"}},
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "foobar.git"},
			err:      codes.Internal,
		},
		{
			desc:     "relative path with directory traversal",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "../bazqux.git"},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "valid path with ..",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "foo../bazqux.git"},
			err:      codes.NotFound, // Because the directory doesn't exist
		},
		{
			desc:     "relative path with sneaky directory traversal",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "/../bazqux.git"},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "relative path with traversal outside storage",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: testRepo.GetRelativePath() + "/../../../../.."},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "relative path with traversal outside storage with trailing slash",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: testRepo.GetRelativePath() + "/../../../../../"},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "relative path with deep traversal at the end",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "bazqux.git/../.."},
			err:      codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			config.Config.Storages = tc.storages
			path, err := GetRepoPath(tc.repo)

			if tc.err != codes.OK {
				testhelper.RequireGrpcError(t, err, tc.err)
				return
			}

			if err != nil {
				assert.NoError(t, err)
				return
			}

			assert.Equal(t, tc.path, path)
		})
	}
}

func assertInvalidRepoWithoutFile(t *testing.T, repo *gitalypb.Repository, repoPath, file string) {
	oldRoute := filepath.Join(repoPath, file)
	renamedRoute := filepath.Join(repoPath, file+"moved")
	os.Rename(oldRoute, renamedRoute)
	defer func() {
		os.Rename(renamedRoute, oldRoute)
	}()

	_, err := GetRepoPath(repo)

	testhelper.RequireGrpcError(t, err, codes.NotFound)
}

func TestGetRepoPathWithCorruptedRepo(t *testing.T) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepoStoragePath := testhelper.GitlabTestStoragePath()
	testRepoPath := filepath.Join(testRepoStoragePath, testRepo.RelativePath)

	for _, file := range []string{"objects", "refs", "HEAD"} {
		assertInvalidRepoWithoutFile(t, testRepo, testRepoPath, file)
	}
}
