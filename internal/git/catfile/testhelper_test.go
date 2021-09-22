package catfile

import (
	"context"
	"os"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer func() {
		testhelper.MustHaveNoChildProcess()
		testhelper.MustHaveNoGoroutines()
	}()

	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

type repoExecutor struct {
	repository.GitRepo
	gitCmdFactory git.CommandFactory
}

func newRepoExecutor(t *testing.T, cfg config.Cfg, repo repository.GitRepo) git.RepositoryExecutor {
	return &repoExecutor{
		GitRepo:       repo,
		gitCmdFactory: git.NewExecCommandFactory(cfg),
	}
}

func (e *repoExecutor) Exec(ctx context.Context, cmd git.Cmd, opts ...git.CmdOpt) (*command.Command, error) {
	return e.gitCmdFactory.New(ctx, e.GitRepo, cmd, opts...)
}

func (e *repoExecutor) ExecAndWait(ctx context.Context, cmd git.Cmd, opts ...git.CmdOpt) error {
	command, err := e.Exec(ctx, cmd, opts...)
	if err != nil {
		return err
	}
	return command.Wait()
}
