package helper

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(m.Run())
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

	testRepo := testhelper.TestRepository()
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
			repo:     &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath},
			path:     repoPath,
		},
		{
			desc: "no storage config, storage name provided",
			repo: &gitalypb.Repository{StorageName: "does not exist", RelativePath: testhelper.TestRelativePath},
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
			repo:     &gitalypb.Repository{StorageName: "default", RelativePath: "made/up/path.git"},
			err:      codes.NotFound,
		},
		{
			desc:     "non existing storage",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: "does not exists", RelativePath: testhelper.TestRelativePath},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "storage defined but storage dir does not exist",
			storages: []config.Storage{{Name: "default", Path: "/does/not/exist"}},
			repo:     &gitalypb.Repository{StorageName: "default", RelativePath: "foobar.git"},
			err:      codes.Internal,
		},
		{
			desc:     "relative path with directory traversal",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: "default", RelativePath: "../bazqux.git"},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "valid path with ..",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: "default", RelativePath: "foo../bazqux.git"},
			err:      codes.NotFound, // Because the directory doesn't exist
		},
		{
			desc:     "relative path with sneaky directory traversal",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: "default", RelativePath: "/../bazqux.git"},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "relative path with traversal outside storage",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath + "/../.."},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "relative path with traversal outside storage with trailing slash",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath + "/../../"},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "relative path with deep traversal at the end",
			storages: exampleStorages,
			repo:     &gitalypb.Repository{StorageName: "default", RelativePath: "bazqux.git/../.."},
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
	testRepo := testhelper.TestRepository()
	testRepoStoragePath := testhelper.GitlabTestStoragePath()
	testRepoPath := filepath.Join(testRepoStoragePath, testRepo.RelativePath)

	for _, file := range []string{"objects", "refs", "HEAD"} {
		assertInvalidRepoWithoutFile(t, testRepo, testRepoPath, file)
	}
}

func TestGetObjectDirectoryPath(t *testing.T) {
	testRepo := testhelper.TestRepository()
	repoPath, err := GetRepoPath(testRepo)
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		desc string
		repo *gitalypb.Repository
		path string
		err  codes.Code
	}{
		{
			desc: "storages configured",
			repo: &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath, GitObjectDirectory: "objects/"},
			path: filepath.Join(repoPath, "objects/"),
		},
		{
			desc: "no GitObjectDirectoryPath",
			repo: &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath},
			err:  codes.InvalidArgument,
		},
		{
			desc: "with directory traversal",
			repo: &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath, GitObjectDirectory: "../bazqux.git"},
			err:  codes.InvalidArgument,
		},
		{
			desc: "valid path but doesn't exist",
			repo: &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath, GitObjectDirectory: "foo../bazqux.git"},
			err:  codes.NotFound,
		},
		{
			desc: "with sneaky directory traversal",
			repo: &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath, GitObjectDirectory: "/../bazqux.git"},
			err:  codes.InvalidArgument,
		},
		{
			desc: "with traversal outside repository",
			repo: &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath, GitObjectDirectory: "objects/../.."},
			err:  codes.InvalidArgument,
		},
		{
			desc: "with traversal outside repository with trailing separator",
			repo: &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath, GitObjectDirectory: "objects/../../"},
			err:  codes.InvalidArgument,
		},
		{
			desc: "with deep traversal at the end",
			repo: &gitalypb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath, GitObjectDirectory: "bazqux.git/../.."},
			err:  codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			path, err := GetObjectDirectoryPath(tc.repo)

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
