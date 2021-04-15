package operations

import (
	"context"
	"os"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	gitalyclient "gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	internalclient "gitlab.com/gitlab-org/gitaly/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

var (
	gitlabPreHooks  = []string{"pre-receive", "update"}
	gitlabPostHooks = []string{"post-receive"}
	GitlabPreHooks  = gitlabPreHooks
	GitlabHooks     []string
)

func init() {
	GitlabHooks = append(GitlabHooks, append(gitlabPreHooks, gitlabPostHooks...)...)
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cleanup := testhelper.Configure()
	defer cleanup()

	return m.Run()
}

func TestWithRubySidecar(t *testing.T) {
	cfg := testcfg.Build(t)

	rubySrv := rubyserver.New(cfg)
	require.NoError(t, rubySrv.Start())
	t.Cleanup(rubySrv.Stop)

	fs := []func(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server){
		testSuccessfulUserApplyPatch,
		testUserApplyPatchStableID,
		testFailedPatchApplyPatch,
		testServerUserCherryPickSuccessful,
		testServerUserCherryPickSuccessfulGitHooks,
		testServerUserCherryPickStableID,
		testServerUserCherryPickFailedValidations,
		testServerUserCherryPickFailedWithPreReceiveError,
		testServerUserCherryPickFailedWithCreateTreeError,
		testServerUserCherryPickFailedWithCommitError,
		testServerUserCherryPickFailedWithConflict,
		testServerUserCherryPickSuccessfulWithGivenCommits,
		testServerUserRevertSuccessful,
		testServerUserRevertStableID,
		testServerUserRevertSuccessfulIntoEmptyRepo,
		testServerUserRevertSuccessfulGitHooks,
		testServerUserRevertFailuedDueToValidations,
		testServerUserRevertFailedDueToPreReceiveError,
		testServerUserRevertFailedDueToCreateTreeErrorConflict,
		testServerUserRevertFailedDueToCommitError,
		testSuccessfulUserUpdateBranchRequestToDelete,
		testSuccessfulGitHooksForUserUpdateBranchRequest,
		testFailedUserUpdateBranchDueToHooks,
		testFailedUserUpdateBranchRequest,
		testSuccessfulUserUpdateBranchRequest,
		testSuccessfulUserRebaseConfirmableRequest,
		testUserRebaseConfirmableTransaction,
		testUserRebaseConfirmableStableCommitIDs,
		testFailedRebaseUserRebaseConfirmableRequestDueToInvalidHeader,
		testAbortedUserRebaseConfirmable,
		testFailedUserRebaseConfirmableDueToApplyBeingFalse,
		testFailedUserRebaseConfirmableRequestDueToPreReceiveError,
		testFailedUserRebaseConfirmableDueToGitError,
		testRebaseRequestWithDeletedFile,
		testRebaseOntoRemoteBranch,
		testSuccessfulUserUpdateSubmoduleRequest,
		testUserUpdateSubmoduleStableID,
		testFailedUserUpdateSubmoduleRequestDueToValidations,
		testFailedUserUpdateSubmoduleRequestDueToInvalidBranch,
		testFailedUserUpdateSubmoduleRequestDueToInvalidSubmodule,
		testFailedUserUpdateSubmoduleRequestDueToSameReference,
		testFailedUserUpdateSubmoduleRequestDueToRepositoryEmpty,
		testServerUserRevertFailedDueToCreateTreeErrorEmpty,
	}
	for _, f := range fs {
		t.Run(runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), func(t *testing.T) {
			f(t, cfg, rubySrv)
		})
	}
}

func setupOperationsService(t testing.TB, ctx context.Context) (context.Context, config.Cfg, *gitalypb.Repository, string, gitalypb.OperationServiceClient) {
	cfg := testcfg.Build(t)

	ctx, cfg, repo, repoPath, client := setupOperationsServiceWithRuby(t, ctx, cfg, nil)

	return ctx, cfg, repo, repoPath, client
}

type setupConfig struct {
	txManagerConstructor func() transaction.Manager
	testServerOpts       []testhelper.TestServerOpt
}

func (c setupConfig) buildTxManager(cfg config.Cfg, registry *backchannel.Registry) transaction.Manager {
	if c.txManagerConstructor != nil {
		return c.txManagerConstructor()
	}
	return transaction.NewManager(cfg, registry)
}

type setupOption func(*setupConfig)

func withTxManagerConstructor(constructor func() transaction.Manager) setupOption {
	return func(config *setupConfig) {
		config.txManagerConstructor = constructor
	}
}

func withTestServerOpts(opts ...testhelper.TestServerOpt) setupOption {
	return func(config *setupConfig) {
		config.testServerOpts = opts
	}
}

func setupOperationsServiceWithRuby(
	t testing.TB, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server, options ...setupOption,
) (context.Context, config.Cfg, *gitalypb.Repository, string, gitalypb.OperationServiceClient) {
	repo, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)

	testhelper.ConfigureGitalySSHBin(t, cfg)
	testhelper.ConfigureGitalyGit2GoBin(t, cfg)
	testhelper.ConfigureGitalyHooksBin(t, cfg)

	serverSocketPath := runOperationServiceServer(t, cfg, rubySrv, options...)
	cfg.SocketPath = serverSocketPath

	client, conn := newOperationClient(t, serverSocketPath)
	t.Cleanup(func() { conn.Close() })

	md := testhelper.GitalyServersMetadata(t, cfg.SocketPath)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	return ctx, cfg, repo, repoPath, client
}

func runOperationServiceServer(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server, options ...setupOption) string {
	t.Helper()

	setupConfig := setupConfig{}
	for _, option := range options {
		option(&setupConfig)
	}

	testServerOpts := append(
		[]testhelper.TestServerOpt{testhelper.WithInternalSocket(cfg)},
		setupConfig.testServerOpts...,
	)

	registry := backchannel.NewRegistry()
	srv := testhelper.NewServerWithAuth(t, nil, nil, cfg.Auth.Token, registry, testServerOpts...)

	conns := gitalyclient.NewPool()
	t.Cleanup(func() { conns.Close() })

	locator := config.NewLocator(cfg)

	txManager := setupConfig.buildTxManager(cfg, registry)
	hookManager := gitalyhook.NewManager(locator, txManager, gitalyhook.GitlabAPIStub, cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	server := NewServer(cfg, rubySrv, hookManager, locator, conns, gitCmdFactory)

	gitalypb.RegisterOperationServiceServer(srv.GrpcServer(), server)
	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hook.NewServer(cfg, hookManager, gitCmdFactory))
	gitalypb.RegisterRepositoryServiceServer(srv.GrpcServer(), repository.NewServer(cfg, rubySrv, locator, txManager, gitCmdFactory))
	gitalypb.RegisterRefServiceServer(srv.GrpcServer(), ref.NewServer(cfg, locator, gitCmdFactory, txManager))
	gitalypb.RegisterCommitServiceServer(srv.GrpcServer(), commit.NewServer(cfg, locator, gitCmdFactory, nil))
	gitalypb.RegisterSSHServiceServer(srv.GrpcServer(), ssh.NewServer(cfg, locator, gitCmdFactory))

	srv.Start(t)
	t.Cleanup(srv.Stop)

	return "unix://" + srv.Socket()
}

func newOperationClient(t testing.TB, serverSocketPath string) (gitalypb.OperationServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewOperationServiceClient(conn), conn
}

func newMuxedOperationClient(t *testing.T, ctx context.Context, serverSocketPath, authToken string, handshaker internalclient.Handshaker) gitalypb.OperationServiceClient {
	conn, err := internalclient.Dial(ctx, serverSocketPath, []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(authToken))}, handshaker)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return gitalypb.NewOperationServiceClient(conn)
}

func setupAndStartGitlabServer(t testing.TB, glID, glRepository string, cfg config.Cfg, gitPushOptions ...string) string {
	url, cleanup := testhelper.SetupAndStartGitlabServer(t, cfg.GitlabShell.Dir, &testhelper.GitlabTestServerOptions{
		SecretToken:                 "secretToken",
		GLID:                        glID,
		GLRepository:                glRepository,
		PostReceiveCounterDecreased: true,
		Protocol:                    "web",
		GitPushOptions:              gitPushOptions,
	})

	t.Cleanup(cleanup)

	return url
}
