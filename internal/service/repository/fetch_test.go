package repository_test

import (
	"net"
	"os"
	"testing"
	"time"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	gitLog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	serverPkg "gitlab.com/gitlab-org/gitaly/internal/server"
	"gitlab.com/gitlab-org/gitaly/internal/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

func TestFetchSourceBranchSourceRepositorySuccess(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	targetRepo, _ := newTestRepo(t, "fetch-source-target.git")
	sourceRepo, sourcePath := newTestRepo(t, "fetch-source-source.git")

	sourceBranch := "fetch-source-branch-test-branch"
	newCommitID := testhelper.CreateCommit(t, sourcePath, sourceBranch, nil)

	targetRef := "refs/tmp/fetch-source-branch-test"
	req := &pb.FetchSourceBranchRequest{
		Repository:       targetRepo,
		SourceRepository: sourceRepo,
		SourceBranch:     []byte(sourceBranch),
		TargetRef:        []byte(targetRef),
	}

	resp, err := client.FetchSourceBranch(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Result, "response.Result should be true")

	fetchedCommit, err := gitLog.GetCommit(ctx, targetRepo, targetRef, "")
	require.NoError(t, err)
	require.Equal(t, newCommitID, fetchedCommit.GetId())
}

func TestFetchSourceBranchSameRepositorySuccess(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	repo, repoPath := newTestRepo(t, "fetch-source-source.git")

	sourceBranch := "fetch-source-branch-test-branch"
	newCommitID := testhelper.CreateCommit(t, repoPath, sourceBranch, nil)

	targetRef := "refs/tmp/fetch-source-branch-test"
	req := &pb.FetchSourceBranchRequest{
		Repository:       repo,
		SourceRepository: repo,
		SourceBranch:     []byte(sourceBranch),
		TargetRef:        []byte(targetRef),
	}

	resp, err := client.FetchSourceBranch(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Result, "response.Result should be true")

	fetchedCommit, err := gitLog.GetCommit(ctx, repo, targetRef, "")
	require.NoError(t, err)
	require.Equal(t, newCommitID, fetchedCommit.GetId())
}

func TestFetchSourceBranchBranchNotFound(t *testing.T) {
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx := metadata.NewOutgoingContext(ctxOuter, md)

	targetRepo, _ := newTestRepo(t, "fetch-source-target.git")
	sourceRepo, _ := newTestRepo(t, "fetch-source-source.git")

	sourceBranch := "does-not-exist"
	targetRef := "refs/tmp/fetch-source-branch-test"

	testCases := []struct {
		req  *pb.FetchSourceBranchRequest
		desc string
	}{
		{
			desc: "target different from source",
			req: &pb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "target same as source",
			req: &pb.FetchSourceBranchRequest{
				Repository:       sourceRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte(targetRef),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			resp, err := client.FetchSourceBranch(ctx, tc.req)
			require.NoError(t, err)
			require.False(t, resp.Result, "response.Result should be false")
		})
	}
}

func TestFetchFullServerRequiresAuthentication(t *testing.T) {
	// The purpose of this test is to ensure that the server started by
	// 'runFullServer' requires authentication. The RPC under test in this
	// file (FetchSourceBranch) makes calls to a "remote" Gitaly server and
	// we want to be sure that authentication is handled correctly. If the
	// tests in this file were using a server without authentication we could
	// not be confident that authentication is done right.
	server, serverSocketPath := runFullServer(t)
	defer server.Stop()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := healthpb.NewHealthClient(conn)
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	testhelper.RequireGrpcError(t, err, codes.Unauthenticated)
}

func newTestRepo(t *testing.T, relativePath string) (*pb.Repository, string) {
	_, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repo := &pb.Repository{StorageName: "default", RelativePath: relativePath}

	repoPath, err := helper.GetPath(repo)
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(repoPath))
	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", testRepoPath, repoPath)

	return repo, repoPath
}

func runFullServer(t *testing.T) (*grpc.Server, string) {
	server := serverPkg.New(repository.RubyServer)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	go server.Serve(listener)

	return server, serverSocketPath
}
