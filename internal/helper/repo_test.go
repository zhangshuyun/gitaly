package helper

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
)

func TestGetRepoPath(t *testing.T) {
	defer func(oldStorages []config.Storage) {
		config.Config.Storages = oldStorages
	}(config.Config.Storages)

	exampleStorages := []config.Storage{
		{Name: "default", Path: testhelper.GitlabTestStoragePath()},
		{Name: "other", Path: "/home/git/repositories2"},
		{Name: "third", Path: "/home/git/repositories3"},
	}

	repoPath := testhelper.GitlabTestRepoPath()

	testCases := []struct {
		desc     string
		storages []config.Storage
		repo     *pb.Repository
		path     string
		err      codes.Code
	}{
		{
			desc:     "storages configured but only repo.Path is provided",
			storages: exampleStorages,
			repo:     &pb.Repository{Path: repoPath},
			path:     repoPath,
		},
		{
			desc:     "storages configured, storage name not known, repo.Path provided",
			storages: exampleStorages,
			repo:     &pb.Repository{Path: repoPath, StorageName: "does not exist", RelativePath: "foobar.git"},
			path:     repoPath,
		},
		{
			desc: "no storages configured, repo.Path provided",
			repo: &pb.Repository{Path: repoPath, StorageName: "does not exist", RelativePath: testhelper.TestRelativePath},
			path: repoPath,
		},
		{
			desc:     "storages configured, no repo.Path",
			storages: exampleStorages,
			repo:     &pb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath},
			path:     repoPath,
		},
		{
			desc:     "storage configured, storage name match, repo.Path provided",
			storages: exampleStorages,
			repo:     &pb.Repository{Path: "/foo/bar.git", StorageName: "default", RelativePath: testhelper.TestRelativePath},
			path:     repoPath,
		},
		{
			desc: "no storage config, repo.Path provided",
			repo: &pb.Repository{Path: repoPath, StorageName: "default", RelativePath: "bazqux.git"},
			path: repoPath,
		},
		{
			desc: "no storage config, storage name provided, no repo.Path",
			repo: &pb.Repository{StorageName: "does not exist", RelativePath: testhelper.TestRelativePath},
			err:  codes.InvalidArgument,
		},
		{
			desc: "no storage config, nil repo",
			err:  codes.InvalidArgument,
		},
		{
			desc:     "storage config provided, empty repo",
			storages: exampleStorages,
			repo:     &pb.Repository{},
			err:      codes.InvalidArgument,
		},
		{
			desc: "no storage config, empty repo",
			repo: &pb.Repository{},
			err:  codes.InvalidArgument,
		},
		{
			desc: "empty string repo",
			repo: &pb.Repository{Path: ""},
			err:  codes.InvalidArgument,
		},
		{
			desc: "non existing repo",
			repo: &pb.Repository{Path: "/made/up/path.git"},
			err:  codes.NotFound,
		},
		{
			desc:     "relative path with directory traversal",
			storages: exampleStorages,
			repo:     &pb.Repository{StorageName: "default", RelativePath: "../bazqux.git"},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "valid path with ..",
			storages: exampleStorages,
			repo:     &pb.Repository{StorageName: "default", RelativePath: "foo../bazqux.git"},
			err:      codes.NotFound, // Because the directory doesn't exist
		},
		{
			desc:     "relative path with sneaky directory traversal",
			storages: exampleStorages,
			repo:     &pb.Repository{StorageName: "default", RelativePath: "/../bazqux.git"},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "relative path with one level traversal at the end",
			storages: exampleStorages,
			repo:     &pb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath + "/.."},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "relative path with one level dashed traversal at the end",
			storages: exampleStorages,
			repo:     &pb.Repository{StorageName: "default", RelativePath: testhelper.TestRelativePath + "/../"},
			err:      codes.InvalidArgument,
		},
		{
			desc:     "relative path with deep traversal at the end",
			storages: exampleStorages,
			repo:     &pb.Repository{StorageName: "default", RelativePath: "bazqux.git/../.."},
			err:      codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		config.Config.Storages = tc.storages
		path, err := GetRepoPath(tc.repo)

		if tc.err != codes.OK {
			testhelper.AssertGrpcError(t, err, tc.err, "")
			continue
		}

		if err != nil {
			assert.NoError(t, err, tc.desc)
			continue
		}

		assert.Equal(t, tc.path, path, tc.desc)
	}
}
