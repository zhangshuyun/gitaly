package repository

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

func TestWriteConfigSuccessful(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testcases := []struct {
		desc    string
		repo    *pb.Repository
		path    string
		setPath string
	}{
		{
			desc:    "valid repo and full_path",
			repo:    testRepo,
			path:    "fullpath.git",
			setPath: "fullpath.git",
		},
		{
			desc:    "empty full_path",
			repo:    testRepo,
			setPath: "fullpath.git", // No change since `nil` is silently ignored
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.WriteConfig(ctx, &pb.WriteConfigRequest{Repository: tc.repo, FullPath: tc.path})
			require.NoError(t, err)
			require.NotNil(t, c)
			require.Empty(t, string(c.GetError()))

			actualConfig := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "gitlab.fullpath")
			require.Equal(t, tc.setPath, strings.TrimRight(string(actualConfig), "\n"))
		})
	}
}

func TestWriteConfigFailure(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testcases := []struct {
		desc string
		repo *pb.Repository
		path string
	}{
		{
			desc: "invalid repo",
			repo: &pb.Repository{StorageName: testhelper.DefaultStorageName, RelativePath: "non-existing.git"},
			path: "non-existing.git",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.WriteConfig(ctx, &pb.WriteConfigRequest{Repository: tc.repo, FullPath: tc.path})
			testhelper.RequireGrpcError(t, err, codes.NotFound)
			require.Nil(t, c)
			require.Empty(t, c.GetError())
		})
	}
}
