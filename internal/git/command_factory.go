package git

import (
	"context"
	"fmt"
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
func NewCommandFactory(cfg config.Cfg) *CommandFactory {
	return &CommandFactory{
		cfg:            cfg,
		locator:        config.NewLocator(cfg),
		cgroupsManager: cgroups.NewManager(cfg.Cgroups),
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
func (cf *CommandFactory) newCommand(ctx context.Context, repo repository.GitRepo, dir string, globals []GlobalOption, sc Cmd, opts ...CmdOpt) (*command.Command, error) {
	cc := &cmdCfg{}

	if err := handleOpts(ctx, sc, cc, opts); err != nil {
		return nil, err
	}

	args, err := combineArgs(globals, sc, cc)
	if err != nil {
		return nil, err
	}

	env := cc.env

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

	command, err := command.New(ctx, execCommand, cc.stdin, cc.stdout, cc.stderr, env...)
	if err != nil {
		return nil, err
	}

	if err := cf.cgroupsManager.AddCommand(command); err != nil {
		return nil, err
	}

	return command, nil
}

func handleOpts(ctx context.Context, sc Cmd, cc *cmdCfg, opts []CmdOpt) error {
	gitCommand, ok := gitCommands[sc.Subcommand()]
	if !ok {
		return fmt.Errorf("invalid sub command name %q: %w", sc.Subcommand(), ErrInvalidArg)
	}

	for _, opt := range opts {
		if err := opt(cc); err != nil {
			return err
		}
	}

	if !cc.hooksConfigured && gitCommand.mayUpdateRef() {
		return fmt.Errorf("subcommand %q: %w", sc.Subcommand(), ErrRefHookRequired)
	}
	if cc.hooksConfigured && !gitCommand.mayUpdateRef() {
		return fmt.Errorf("subcommand %q: %w", sc.Subcommand(), ErrRefHookNotRequired)
	}
	if gitCommand.mayGeneratePackfiles() {
		cc.globals = append(cc.globals, ConfigPair{
			Key: "pack.windowMemory", Value: "100m",
		})
	}

	return nil
}

func combineArgs(globals []GlobalOption, sc Cmd, cc *cmdCfg) (_ []string, err error) {
	var args []string

	defer func() {
		if err != nil && IsInvalidArgErr(err) && len(args) > 0 {
			incrInvalidArg(args[0])
		}
	}()

	gitCommand, ok := gitCommands[sc.Subcommand()]
	if !ok {
		return nil, fmt.Errorf("invalid sub command name %q: %w", sc.Subcommand(), ErrInvalidArg)
	}

	// As global options may cancel out each other, we have a clearly
	// defined order in which globals get applied. The order is similar to
	// how git handles configuration options from most general to most
	// specific. This allows callsites to override options which would
	// otherwise be set up automatically.
	//
	// 1. Globals which get set up by default for a given git command.
	// 2. Globals passed via command options, e.g. as set up by
	//    `WithReftxHook()`.
	// 3. Globals passed directly to the command at the site of execution.
	var combinedGlobals []GlobalOption
	combinedGlobals = append(combinedGlobals, gitCommand.opts...)
	combinedGlobals = append(combinedGlobals, cc.globals...)
	combinedGlobals = append(combinedGlobals, globals...)

	for _, global := range combinedGlobals {
		globalArgs, err := global.GlobalArgs()
		if err != nil {
			return nil, err
		}
		args = append(args, globalArgs...)
	}

	scArgs, err := sc.CommandArgs()
	if err != nil {
		return nil, err
	}

	return append(args, scArgs...), nil
}
