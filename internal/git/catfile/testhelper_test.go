package catfile

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
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

func setupObjectReader(t *testing.T, ctx context.Context) (config.Cfg, ObjectReader, *gitalypb.Repository) {
	t.Helper()

	cfg, repo, _ := testcfg.BuildWithRepo(t)
	repoExecutor := newRepoExecutor(t, cfg, repo)

	cache := newCache(1*time.Hour, 1000, defaultEvictionInterval)
	t.Cleanup(cache.Stop)

	objectReader, err := cache.ObjectReader(ctx, repoExecutor)
	require.NoError(t, err)

	return cfg, objectReader, repo
}
