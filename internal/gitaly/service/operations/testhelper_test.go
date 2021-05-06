package operations

import (
	"context"
	"os"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	internalclient "gitlab.com/gitlab-org/gitaly/internal/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
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
		testRebaseFailedWithCode,
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

func setupOperationsServiceWithRuby(
	t testing.TB, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server, options ...testserver.GitalyServerOpt,
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

	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	return ctx, cfg, repo, repoPath, client
}

func runOperationServiceServer(t testing.TB, cfg config.Cfg, rubySrv *rubyserver.Server, options ...testserver.GitalyServerOpt) string {
	t.Helper()

	return testserver.RunGitalyServer(t, cfg, rubySrv, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterOperationServiceServer(srv, NewServer(deps.GetCfg(), deps.GetRubyServer(), deps.GetHookManager(), deps.GetLocator(), deps.GetConnsPool(), deps.GetGitCmdFactory()))
		gitalypb.RegisterHookServiceServer(srv, hook.NewServer(cfg, deps.GetHookManager(), deps.GetGitCmdFactory()))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(cfg, rubySrv, deps.GetLocator(), deps.GetTxManager(), deps.GetGitCmdFactory()))
		gitalypb.RegisterRefServiceServer(srv, ref.NewServer(cfg, deps.GetLocator(), deps.GetGitCmdFactory(), deps.GetTxManager()))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(cfg, deps.GetLocator(), deps.GetGitCmdFactory(), nil))
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(
			cfg,
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
		))
	}, options...)
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
