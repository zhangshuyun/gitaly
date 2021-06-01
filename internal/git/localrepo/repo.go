package localrepo

import (
	"context"
	"fmt"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
)

// Repo represents a local Git repository.
type Repo struct {
	repository.GitRepo
	gitCmdFactory git.CommandFactory
	cfg           config.Cfg
	locator       storage.Locator
	catfileCache  catfile.Cache
}

// New creates a new Repo from its protobuf representation.
func New(gitCmdFactory git.CommandFactory, catfileCache catfile.Cache, repo repository.GitRepo, cfg config.Cfg) *Repo {
	return &Repo{
		GitRepo:       repo,
		cfg:           cfg,
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,
		locator:       config.NewLocator(cfg),
	}
}

// NewTestRepo constructs a Repo. It is intended as a helper function for tests which assembles
// dependencies ad-hoc from the given config.
func NewTestRepo(t testing.TB, cfg config.Cfg, repo repository.GitRepo) *Repo {
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	return New(gitCmdFactory, catfile.NewCache(cfg), repo, cfg)
}

// Path returns the on-disk path of the repository.
func (repo *Repo) Path() (string, error) {
	return repo.locator.GetRepoPath(repo)
}

// Exec creates a git command with the given args and Repo, executed in the
// Repo. It validates the arguments in the command before executing.
func (repo *Repo) Exec(ctx context.Context, cmd git.Cmd, opts ...git.CmdOpt) (*command.Command, error) {
	return repo.gitCmdFactory.New(ctx, repo, cmd, opts...)
}

// ExecAndWait is similar to Exec, but waits for the command to exit before
// returning.
func (repo *Repo) ExecAndWait(ctx context.Context, cmd git.Cmd, opts ...git.CmdOpt) error {
	command, err := repo.Exec(ctx, cmd, opts...)
	if err != nil {
		return err
	}

	return command.Wait()
}

// Config returns executor of the 'config' sub-command.
func (repo *Repo) Config() Config {
	return Config{repo: repo}
}

// Remote returns executor of the 'remote' sub-command.
func (repo *Repo) Remote() Remote {
	return Remote{repo: repo}
}

func errorWithStderr(err error, stderr []byte) error {
	return fmt.Errorf("%w, stderr: %q", err, stderr)
}
