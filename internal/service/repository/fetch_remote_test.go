package repository

import (
	"os"
	"path"
	"testing"

	"google.golang.org/grpc/codes"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func copyRepoWithNewRemote(t *testing.T, repo *pb.Repository, remote string) *pb.Repository {
	repoPath, err := helper.GetRepoPath(repo)
	require.NoError(t, err)

	cloneRepo := &pb.Repository{StorageName: repo.GetStorageName(), RelativePath: "fetch-remote-clone.git"}

	clonePath := path.Join(testhelper.GitlabTestStoragePath(), "fetch-remote-clone.git")
	t.Logf("clonePath: %q", clonePath)
	os.RemoveAll(clonePath)

	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", repoPath, clonePath)

	testhelper.MustRunCommand(t, nil, "git", "-C", clonePath, "remote", "add", remote, repoPath)

	return cloneRepo
}

func TestFetchRemoteSuccess(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, _ := newRepositoryClient(t, serverSocketPath)

	cloneRepo := copyRepoWithNewRemote(t, testRepo, "my-remote")
	defer func(r *pb.Repository) {
		path, err := helper.GetRepoPath(r)
		if err != nil {
			panic(err)
		}
		os.RemoveAll(path)
	}(cloneRepo)

	resp, err := client.FetchRemote(ctx, &pb.FetchRemoteRequest{
		Repository: cloneRepo,
		Remote:     "my-remote",
		Timeout:    120,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestFetchRemoteFailure(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, _ := newRepositoryClient(t, serverSocketPath)

	tests := []struct {
		desc string
		req  *pb.FetchRemoteRequest
		code codes.Code
		err  string
	}{
		{
			desc: "invalid storage",
			req:  &pb.FetchRemoteRequest{Repository: &pb.Repository{StorageName: "invalid", RelativePath: "foobar.git"}},
			code: codes.InvalidArgument,
			err:  "Storage can not be found by name 'invalid'",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			resp, err := client.FetchRemote(ctx, tc.req)
			testhelper.RequireGrpcError(t, err, tc.code)
			assert.Error(t, err)
			assert.Nil(t, resp)
		})
	}
}
