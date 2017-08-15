package repository

import (
	"io/ioutil"
	"os"
	"testing"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestRepositoryExists(t *testing.T) {
	server := runRepoServer(t)
	defer server.Stop()

	storageOtherDir, err := ioutil.TempDir("", "gitaly-repository-exists-test")
	require.NoError(t, err, "tempdir")
	defer os.Remove(storageOtherDir)

	client, conn := newRepositoryClient(t)
	defer conn.Close()

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

	queries := []struct {
		desc      string
		request   *pb.RepositoryExistsRequest
		errorCode codes.Code
		exists    bool
	}{
		{
			desc: "repository nil",
			request: &pb.RepositoryExistsRequest{
				Repository: nil,
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "storage name empty",
			request: &pb.RepositoryExistsRequest{
				Repository: &pb.Repository{
					StorageName:  "",
					RelativePath: testhelper.TestRelativePath,
				},
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "relative path empty",
			request: &pb.RepositoryExistsRequest{
				Repository: &pb.Repository{
					StorageName:  "default",
					RelativePath: "",
				},
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "exists true",
			request: &pb.RepositoryExistsRequest{
				Repository: &pb.Repository{
					StorageName:  "default",
					RelativePath: testhelper.TestRelativePath,
				},
			},
			exists: true,
		},
		{
			desc: "exists false, wrong storage",
			request: &pb.RepositoryExistsRequest{
				Repository: &pb.Repository{
					StorageName:  "other",
					RelativePath: testhelper.TestRelativePath,
				},
			},
			exists: false,
		},
		{
			desc: "storage directory does not exist",
			request: &pb.RepositoryExistsRequest{
				Repository: &pb.Repository{
					StorageName:  "broken",
					RelativePath: "foobar.git",
				},
			},
			errorCode: codes.Internal,
		},
	}

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response, err := client.Exists(ctx, tc.request)

			require.Equal(t, tc.errorCode, grpc.Code(err))

			if err != nil {
				// Ignore the response message if there was an error
				return
			}

			require.Equal(t, tc.exists, response.Exists)
		})
	}
}
