package repository

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

func copyRepoWithNewRemote(t *testing.T, repo *gitalypb.Repository, remote string) *gitalypb.Repository {
	repoPath, err := helper.GetRepoPath(repo)
	require.NoError(t, err)

	cloneRepo := &gitalypb.Repository{StorageName: repo.GetStorageName(), RelativePath: "fetch-remote-clone.git"}

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
	defer func(r *gitalypb.Repository) {
		path, err := helper.GetRepoPath(r)
		if err != nil {
			panic(err)
		}
		os.RemoveAll(path)
	}(cloneRepo)

	resp, err := client.FetchRemote(ctx, &gitalypb.FetchRemoteRequest{
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
		req  *gitalypb.FetchRemoteRequest
		code codes.Code
		err  string
	}{
		{
			desc: "invalid storage",
			req:  &gitalypb.FetchRemoteRequest{Repository: &gitalypb.Repository{StorageName: "invalid", RelativePath: "foobar.git"}},
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
			require.Contains(t, err.Error(), tc.err)
			assert.Nil(t, resp)
		})
	}
}
