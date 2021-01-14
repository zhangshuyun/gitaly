package git

import (
	"context"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
)

// CommandFactory knows how to properly construct different types of commands.
type CommandFactory struct {
	locator        storage.Locator
	cfg            config.Cfg
	cgroupsManager cgroups.Manager
}

// NewCommandFactory returns a new instance of initialized CommandFactory.
// Current implementation relies on the global var 'config.Config' and a single type of 'Locator' we currently have.
// This dependency will be removed on the next iterations in scope of: https://gitlab.com/gitlab-org/gitaly/-/issues/2699
func NewCommandFactory() *CommandFactory {
	return &CommandFactory{
		cfg:            config.Config,
		locator:        config.NewLocator(config.Config),
		cgroupsManager: cgroups.NewManager(config.Config.Cgroups),
	}
}

func (cf *CommandFactory) gitPath() string {
	return cf.cfg.Git.BinPath
}

// unsafeCmd creates a git.unsafeCmd with the given args, environment, and Repository
func (cf *CommandFactory) unsafeCmd(ctx context.Context, extraEnv []string, stream cmdStream, repo repository.GitRepo, args ...string) (*command.Command, error) {
	return cf.newCommand(ctx, repo, stream, "", extraEnv, args...)
}

// unsafeBareCmd creates a git.Command with the given args, stdin/stdout/stderr, and env
func (cf *CommandFactory) unsafeBareCmd(ctx context.Context, stream cmdStream, env []string, args ...string) (*command.Command, error) {
	return cf.newCommand(ctx, nil, stream, "", env, args...)
}

// unsafeBareCmdInDir call sunsafeBareCmd in dir.
func (cf *CommandFactory) unsafeBareCmdInDir(ctx context.Context, dir string, stream cmdStream, env []string, args ...string) (*command.Command, error) {
	return cf.newCommand(ctx, nil, stream, dir, env, args...)
}

func (cf *CommandFactory) newCommand(ctx context.Context, repo repository.GitRepo, stream cmdStream, dir string, env []string, args ...string) (*command.Command, error) {
	if repo != nil {
		repoPath, err := cf.locator.GetRepoPath(repo)
		if err != nil {
			return nil, err
		}

		env = append(alternates.Env(repoPath, repo.GetGitObjectDirectory(), repo.GetGitAlternateObjectDirectories()), env...)
		args = append([]string{"--git-dir", repoPath}, args...)
	}

	env = append(env, command.GitEnv...)

	execCommand := exec.Command(cf.gitPath(), args...)
	execCommand.Dir = dir

	command, err := command.New(ctx, execCommand, stream.In, stream.Out, stream.Err, env...)
	if err != nil {
		return nil, err
	}

	if err := cf.cgroupsManager.AddCommand(command); err != nil {
		return nil, err
	}

	return command, nil
}
