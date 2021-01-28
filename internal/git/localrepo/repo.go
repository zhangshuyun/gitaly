package localrepo

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

// Repo represents a local Git repository.
type Repo struct {
	repository.GitRepo
	commandFactory *git.ExecCommandFactory
	cfg            config.Cfg
}

// New creates a new Repo from its protobuf representation.
func New(repo repository.GitRepo, cfg config.Cfg) *Repo {
	return &Repo{
		GitRepo:        repo,
		cfg:            cfg,
		commandFactory: git.NewExecCommandFactory(cfg),
	}
}

// command creates a Git Command with the given args and Repository, executed
// in the Repository. It validates the arguments in the command before
// executing.
func (repo *Repo) command(ctx context.Context, globals []git.GlobalOption, cmd git.Cmd, opts ...git.CmdOpt) (*command.Command, error) {
	return repo.commandFactory.New(ctx, repo, globals, cmd, opts...)
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
