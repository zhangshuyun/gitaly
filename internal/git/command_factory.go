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

// newCommand creates a new command.Command for the given git command and
// global options. If a repo is given, then the command will be run in the
// context of that repository. Note that this sets up arguments and environment
// variables for git, but doesn't run in the directory itself. If a directory
// is given, then the command will be run in that directory.
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
