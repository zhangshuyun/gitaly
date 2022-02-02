package conflicts

import (
	"context"
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/commit"
	hookservice "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ssh"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func SetupConflictsService(ctx context.Context, t testing.TB, bare bool, hookManager hook.Manager) (config.Cfg, *gitalypb.Repository, string, gitalypb.ConflictsServiceClient) {
	cfg := testcfg.Build(t)

	testcfg.BuildGitalyGit2Go(t, cfg)

	serverSocketPath := runConflictsServer(t, cfg, hookManager)
	cfg.SocketPath = serverSocketPath

	client, conn := NewConflictsClient(t, serverSocketPath)
	t.Cleanup(func() { conn.Close() })

	repo, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	if !bare {
		gittest.AddWorktree(t, cfg, repoPath, "worktree")
		repoPath = filepath.Join(repoPath, "worktree")
		// AddWorktree creates a detached worktree. Checkout master here so the
		// branch pointer moves as we later commit.
		gittest.Exec(t, cfg, "-C", repoPath, "checkout", "master")
	}

	return cfg, repo, repoPath, client
}

func runConflictsServer(t testing.TB, cfg config.Cfg, hookManager hook.Manager) string {
	return testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterConflictsServiceServer(srv, NewServer(
			deps.GetHookManager(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
			deps.GetUpdaterWithHooks(),
		))
		gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
			deps.GetConnsPool(),
			deps.GetGit2goExecutor(),
		))
		gitalypb.RegisterSSHServiceServer(srv, ssh.NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps.GetHookManager(), deps.GetGitCmdFactory(), deps.GetPackObjectsCache()))
		gitalypb.RegisterCommitServiceServer(srv, commit.NewServer(
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetLinguist(),
			deps.GetCatfileCache(),
		))
	}, testserver.WithHookManager(hookManager), testserver.WithDisableMetadataForceCreation())
}

func NewConflictsClient(t testing.TB, serverSocketPath string) (gitalypb.ConflictsServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewConflictsServiceClient(conn), conn
}
