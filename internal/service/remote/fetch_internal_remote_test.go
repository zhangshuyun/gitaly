package remote_test

import (
	"context"
	"net"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/service/remote"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/stretchr/testify/require"
	serverPkg "gitlab.com/gitlab-org/gitaly/internal/server"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
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

	request := &pb.FetchInternalRemoteRequest{
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
	remoteRepo := &pb.Repository{StorageName: "default", RelativePath: "fake.git"}

	request := &pb.FetchInternalRemoteRequest{
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

	repo := &pb.Repository{StorageName: "default", RelativePath: "repo.git"}

	testCases := []struct {
		desc    string
		request *pb.FetchInternalRemoteRequest
	}{
		{
			desc:    "empty Repository",
			request: &pb.FetchInternalRemoteRequest{RemoteRepository: repo},
		},
		{
			desc:    "empty Remote Repository",
			request: &pb.FetchInternalRemoteRequest{Repository: repo},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.FetchInternalRemote(ctx, tc.request)

			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
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
