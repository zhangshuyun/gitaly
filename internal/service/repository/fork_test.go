package repository_test

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestSuccessfulCreateForkRequest(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	forkedRepo := &pb.Repository{
		RelativePath: "forks/test-repo-fork.git",
		StorageName:  testRepo.StorageName,
	}

	req := &pb.CreateForkRequest{
		Repository:       forkedRepo,
		SourceRepository: testRepo,
	}

	_, err := client.CreateFork(ctx, req)
	require.NoError(t, err)

	forkedRepoPath, err := helper.GetRepoPath(forkedRepo)
	require.NoError(t, err)
	defer os.RemoveAll(forkedRepoPath)

	testhelper.MustRunCommand(t, nil, "git", "-C", forkedRepoPath, "fsck")

	remotes := testhelper.MustRunCommand(t, nil, "git", "-C", forkedRepoPath, "remote")
	require.NotContains(t, string(remotes), "origin")

	info, err := os.Lstat(path.Join(forkedRepoPath, "hooks"))
	require.NoError(t, err)
	require.NotEqual(t, 0, info.Mode()&os.ModeSymlink)
}

func TestFailedCreateForkRequestDueToExistingTarget(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc     string
		repoPath string
		isDir    bool
	}{
		{
			desc:     "target is a directory",
			repoPath: "forks/test-repo-fork-dir.git",
			isDir:    true,
		},
		{
			desc:     "target is a file",
			repoPath: "forks/test-repo-fork-file.git",
			isDir:    false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			forkedRepo := &pb.Repository{
				RelativePath: testCase.repoPath,
				StorageName:  testRepo.StorageName,
			}

			forkedRepoPath, err := helper.GetPath(forkedRepo)
			require.NoError(t, err)

			if testCase.isDir {
				require.NoError(t, os.MkdirAll(forkedRepoPath, 0770))
			} else {
				require.NoError(t, ioutil.WriteFile(forkedRepoPath, nil, 0644))
			}
			defer os.RemoveAll(forkedRepoPath)

			req := &pb.CreateForkRequest{
				Repository:       forkedRepo,
				SourceRepository: testRepo,
			}

			_, err = client.CreateFork(ctx, req)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}
