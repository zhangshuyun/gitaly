package git

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

// LocalRepository represents a local Git repository.
type LocalRepository struct {
	repo           repository.GitRepo
	commandFactory *ExecCommandFactory
	cfg            config.Cfg
}

// NewRepository creates a new Repository from its protobuf representation.
func NewRepository(repo repository.GitRepo, cfg config.Cfg) *LocalRepository {
	return &LocalRepository{
		repo:           repo,
		cfg:            cfg,
		commandFactory: NewExecCommandFactory(cfg),
	}
}

// command creates a Git Command with the given args and Repository, executed
// in the Repository. It validates the arguments in the command before
// executing.
func (repo *LocalRepository) command(ctx context.Context, globals []GlobalOption, cmd SubCmd, opts ...CmdOpt) (*command.Command, error) {
	return repo.commandFactory.newCommand(ctx, repo.repo, "", globals, cmd, opts...)
}

// Config returns executor of the 'config' sub-command.
func (repo *LocalRepository) Config() Config {
	return LocalRepositoryConfig{repo: repo.repo}
}

// Remote returns executor of the 'remote' sub-command.
func (repo *LocalRepository) Remote() Remote {
	return LocalRepositoryRemote{repo: repo.repo}
}

func errorWithStderr(err error, stderr []byte) error {
	return fmt.Errorf("%w, stderr: %q", err, stderr)
}
