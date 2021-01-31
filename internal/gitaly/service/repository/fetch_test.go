package repository_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	gitLog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	serverPkg "gitlab.com/gitlab-org/gitaly/internal/gitaly/server"
	hookservice "gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestFetchSourceBranchSourceRepositorySuccess(t *testing.T) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, clean := runFullServer(t)
	defer clean()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	targetRepo, _, cleanup := newTestRepo(t, locator, "fetch-source-target.git")
	defer cleanup()

	sourceRepo, sourcePath, cleanup := newTestRepo(t, locator, "fetch-source-source.git")
	defer cleanup()

	sourceBranch := "fetch-source-branch-test-branch"
	newCommitID := testhelper.CreateCommit(t, sourcePath, sourceBranch, nil)

	targetRef := "refs/tmp/fetch-source-branch-test"
	req := &gitalypb.FetchSourceBranchRequest{
		Repository:       targetRepo,
		SourceRepository: sourceRepo,
		SourceBranch:     []byte(sourceBranch),
		TargetRef:        []byte(targetRef),
	}

	resp, err := client.FetchSourceBranch(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Result, "response.Result should be true")

	fetchedCommit, err := gitLog.GetCommit(ctx, locator, targetRepo, git.Revision(targetRef))
	require.NoError(t, err)
	require.Equal(t, newCommitID, fetchedCommit.GetId())
}

func TestFetchSourceBranchSameRepositorySuccess(t *testing.T) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, clean := runFullServer(t)
	defer clean()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	repo, repoPath, cleanup := newTestRepo(t, locator, "fetch-source-source.git")
	defer cleanup()

	sourceBranch := "fetch-source-branch-test-branch"
	newCommitID := testhelper.CreateCommit(t, repoPath, sourceBranch, nil)

	targetRef := "refs/tmp/fetch-source-branch-test"
	req := &gitalypb.FetchSourceBranchRequest{
		Repository:       repo,
		SourceRepository: repo,
		SourceBranch:     []byte(sourceBranch),
		TargetRef:        []byte(targetRef),
	}

	resp, err := client.FetchSourceBranch(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Result, "response.Result should be true")

	fetchedCommit, err := gitLog.GetCommit(ctx, locator, repo, git.Revision(targetRef))
	require.NoError(t, err)
	require.Equal(t, newCommitID, fetchedCommit.GetId())
}

func TestFetchSourceBranchBranchNotFound(t *testing.T) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, clean := runFullServer(t)
	defer clean()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	targetRepo, _, cleanup := newTestRepo(t, locator, "fetch-source-target.git")
	defer cleanup()

	sourceRepo, _, cleanup := newTestRepo(t, locator, "fetch-source-source.git")
	defer cleanup()

	sourceBranch := "does-not-exist"
	targetRef := "refs/tmp/fetch-source-branch-test"

	testCases := []struct {
		req  *gitalypb.FetchSourceBranchRequest
		desc string
	}{
		{
			desc: "target different from source",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "target same as source",
			req: &gitalypb.FetchSourceBranchRequest{
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

func TestFetchSourceBranchWrongRef(t *testing.T) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, clean := runFullServer(t)
	defer clean()

	client, conn := repository.NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadata(t, serverSocketPath)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	targetRepo, _, cleanup := newTestRepo(t, locator, "fetch-source-target.git")
	defer cleanup()

	sourceRepo, sourceRepoPath, cleanup := newTestRepo(t, locator, "fetch-source-source.git")
	defer cleanup()

	sourceBranch := "fetch-source-branch-testmas-branch"
	testhelper.CreateCommit(t, sourceRepoPath, sourceBranch, nil)

	targetRef := "refs/tmp/fetch-source-branch-test"

	testCases := []struct {
		req  *gitalypb.FetchSourceBranchRequest
		desc string
	}{
		{
			desc: "source branch empty",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(""),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "source branch blank",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte("   "),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "source branch starts with -",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte("-ref"),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "source branch with :",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte("some:ref"),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "source branch with NULL",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte("some\x00ref"),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "target branch empty",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte(""),
			},
		},
		{
			desc: "target branch blank",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte("   "),
			},
		},
		{
			desc: "target branch starts with -",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte("-ref"),
			},
		},
		{
			desc: "target branch with :",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte("some:ref"),
			},
		},
		{
			desc: "target branch with NULL",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte("some\x00ref"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.FetchSourceBranch(ctx, tc.req)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
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
	serverSocketPath, clean := runFullServer(t)
	defer clean()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
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

func newTestRepo(t *testing.T, locator storage.Locator, relativePath string) (*gitalypb.Repository, string, func()) {
	_, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repo := &gitalypb.Repository{StorageName: "default", RelativePath: relativePath}

	repoPath, err := locator.GetPath(repo)
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(repoPath))
	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", testRepoPath, repoPath)

	return repo, repoPath, func() { require.NoError(t, os.RemoveAll(repoPath)) }
}

func runFullServer(t *testing.T) (string, func()) {
	return testserver.RunGitalyServer(t, config.Config, repository.RubyServer)
}

func runFullSecureServer(t *testing.T, locator storage.Locator) (*grpc.Server, string, testhelper.Cleanup) {
	t.Helper()

	conns := client.NewPool()
	cfg := config.Config
	txManager := transaction.NewManager(cfg)
	hookManager := hook.NewManager(locator, txManager, hook.GitlabAPIStub, cfg)

	server, err := serverPkg.New(true, repository.RubyServer, hookManager, txManager, cfg, conns, config.NewLocator(cfg), git.NewExecCommandFactory(cfg))
	require.NoError(t, err)
	listener, addr := testhelper.GetLocalhostListener(t)

	errQ := make(chan error)

	// This creates a secondary GRPC server which isn't "secure". Reusing
	// the one created above won't work as its internal socket would be
	// protected by the same TLS certificate.
	internalServer := testhelper.NewServer(t, nil, nil, testhelper.WithInternalSocket(cfg))
	gitalypb.RegisterHookServiceServer(internalServer.GrpcServer(), hookservice.NewServer(cfg, hookManager))
	internalServer.Start(t)

	go func() { errQ <- server.Serve(listener) }()

	cleanup := func() {
		conns.Close()
		server.Stop()
		internalServer.Stop()
		require.NoError(t, <-errQ)
	}

	return server, "tls://" + addr, cleanup
}
