package repository

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestRepositoryExists(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	storageOtherDir, err := ioutil.TempDir("", "gitaly-repository-exists-test")
	require.NoError(t, err, "tempdir")
	defer os.Remove(storageOtherDir)

	client, conn := newRepositoryClient(t, serverSocketPath)
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
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	emptyRepoName := "empty-repo.git"
	emptyRepoPath := path.Join(testhelper.GitlabTestStoragePath(), emptyRepoName)
	testhelper.MustRunCommand(t, nil, "git", "init", "--bare", emptyRepoPath)
	defer os.RemoveAll(emptyRepoPath)

	testCases := []struct {
		desc      string
		request   *pb.HasLocalBranchesRequest
		value     bool
		errorCode codes.Code
	}{
		{
			desc:    "repository has branches",
			request: &pb.HasLocalBranchesRequest{Repository: testRepo},
			value:   true,
		},
		{
			desc: "repository doesn't have branches",
			request: &pb.HasLocalBranchesRequest{
				Repository: &pb.Repository{
					StorageName:  "default",
					RelativePath: emptyRepoName,
				},
			},
			value: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
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

func BenchmarkHasLocalBranchesShell(b *testing.B) {
	defer testhelper.DisableLogs()()
	server, serverSocketPath := runRepoServer(b)
	defer server.Stop()

	client, conn := newRepositoryClient(b, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(b)
	defer cleanupFn()

	for n := 0; n < 10000; n++ {
		testhelper.MustRunCommand(b, nil, "git", fmt.Sprintf("--git-dir=%s", testRepoPath), "update-ref", fmt.Sprintf("refs/test/%d", n), "refs/heads/master")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		resp, err := client.HasLocalBranches(ctx, &pb.HasLocalBranchesRequest{Repository: testRepo})

		require.NoError(b, err)
		require.True(b, resp.Value)
	}
}

func BenchmarkHasLocalBranchesRuby(b *testing.B) {
	hlbRuby = true
	defer func() { hlbRuby = false }()

	BenchmarkHasLocalBranchesShell(b)

}

func TestFailedHasLocalBranches(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc       string
		repository *pb.Repository
		errorCode  codes.Code
	}{
		{
			desc:       "repository nil",
			repository: nil,
			errorCode:  codes.InvalidArgument,
		},
		{
			desc:       "repository doesn't exist",
			repository: &pb.Repository{StorageName: "fake", RelativePath: "path"},
			errorCode:  codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			request := &pb.HasLocalBranchesRequest{Repository: tc.repository}
			_, err := client.HasLocalBranches(ctx, request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))
		})
	}
}
