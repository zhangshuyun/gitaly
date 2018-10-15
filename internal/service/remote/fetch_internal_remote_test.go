package remote_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	serverPkg "gitlab.com/gitlab-org/gitaly/internal/server"
	"gitlab.com/gitlab-org/gitaly/internal/service/remote"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestSuccessfulFetchInternalRemote(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := remote.NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	remoteRepo, remoteRepoPath, remoteCleanupFn := testhelper.NewTestRepo(t)
	defer remoteCleanupFn()

	repo, repoPath, cleanupFn := testhelper.InitBareRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	request := &gitalypb.FetchInternalRemoteRequest{
		Repository:       repo,
		RemoteRepository: remoteRepo,
	}

	c, err := client.FetchInternalRemote(ctx, request)
	require.NoError(t, err)
	require.True(t, c.GetResult())

	remoteRefs := testhelper.GetRepositoryRefs(t, remoteRepoPath)
	refs := testhelper.GetRepositoryRefs(t, repoPath)
	require.Equal(t, remoteRefs, refs)
}

func TestFailedFetchInternalRemote(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := remote.NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	repo, _, cleanupFn := testhelper.InitBareRepo(t)
	defer cleanupFn()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	// Non-existing remote repo
	remoteRepo := &gitalypb.Repository{StorageName: "default", RelativePath: "fake.git"}

	request := &gitalypb.FetchInternalRemoteRequest{
		Repository:       repo,
		RemoteRepository: remoteRepo,
	}

	c, err := client.FetchInternalRemote(ctx, request)
	require.NoError(t, err)
	require.False(t, c.GetResult())
}

func TestFailedFetchInternalRemoteDueToValidations(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := remote.NewRemoteClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := &gitalypb.Repository{StorageName: "default", RelativePath: "repo.git"}

	testCases := []struct {
		desc    string
		request *gitalypb.FetchInternalRemoteRequest
	}{
		{
			desc:    "empty Repository",
			request: &gitalypb.FetchInternalRemoteRequest{RemoteRepository: repo},
		},
		{
			desc:    "empty Remote Repository",
			request: &gitalypb.FetchInternalRemoteRequest{Repository: repo},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.FetchInternalRemote(ctx, tc.request)

			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
		})
	}
}

func runFullServer(t *testing.T) (*grpc.Server, string) {
	server := serverPkg.New(remote.RubyServer)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	go server.Serve(listener)

	return server, serverSocketPath
}
