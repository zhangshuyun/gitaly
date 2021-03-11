package git

import (
	"context"
	"errors"
	"fmt"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
)

var (
	globalOptions = []GlobalOption{
		// Synchronize object files to lessen the likelihood of
		// repository corruption in case the server crashes.
		ConfigPair{Key: "core.fsyncObjectFiles", Value: "true"},

		// Disable automatic garbage collection as we handle scheduling
		// of it ourselves.
		ConfigPair{Key: "gc.auto", Value: "0"},

		// CRLF line endings will get replaced with LF line endings
		// when writing blobs to the object database. No conversion is
		// done when reading blobs from the object database. This is
		// required for the web editor.
		ConfigPair{Key: "core.autocrlf", Value: "input"},
	}
)

// CommandFactory is designed to create and run git commands in a protected and fully managed manner.
type CommandFactory interface {
	// New creates a new command for the repo repository.
	New(ctx context.Context, repo repository.GitRepo, sc Cmd, opts ...CmdOpt) (*command.Command, error)
	// NewWithoutRepo creates a command without a target repository.
	NewWithoutRepo(ctx context.Context, sc Cmd, opts ...CmdOpt) (*command.Command, error)
	// NewWithDir creates a command without a target repository that would be executed in dir directory.
	NewWithDir(ctx context.Context, dir string, sc Cmd, opts ...CmdOpt) (*command.Command, error)
}

// ExecCommandFactory knows how to properly construct different types of commands.
type ExecCommandFactory struct {
	locator        storage.Locator
	cfg            config.Cfg
	cgroupsManager cgroups.Manager
}

// NewExecCommandFactory returns a new instance of initialized ExecCommandFactory.
func NewExecCommandFactory(cfg config.Cfg) *ExecCommandFactory {
	return &ExecCommandFactory{
		cfg:            cfg,
		locator:        config.NewLocator(cfg),
		cgroupsManager: cgroups.NewManager(cfg.Cgroups),
	}
}

// New creates a new command for the repo repository.
func (cf *ExecCommandFactory) New(ctx context.Context, repo repository.GitRepo, sc Cmd, opts ...CmdOpt) (*command.Command, error) {
	return cf.newCommand(ctx, repo, "", nil, sc, opts...)
}

// NewWithoutRepo creates a command without a target repository.
func (cf *ExecCommandFactory) NewWithoutRepo(ctx context.Context, sc Cmd, opts ...CmdOpt) (*command.Command, error) {
	return cf.newCommand(ctx, nil, "", nil, sc, opts...)
}

// NewWithDir creates a new command.Command whose working directory is set
// to dir. Arguments are validated before the command is being run. It is
// invalid to use an empty directory.
func (cf *ExecCommandFactory) NewWithDir(ctx context.Context, dir string, sc Cmd, opts ...CmdOpt) (*command.Command, error) {
	if dir == "" {
		return nil, errors.New("no 'dir' provided")
	}

	return cf.newCommand(ctx, nil, dir, nil, sc, opts...)
}

func (cf *ExecCommandFactory) gitPath() string {
	return cf.cfg.Git.BinPath
}

// newCommand creates a new command.Command for the given git command and
// global options. If a repo is given, then the command will be run in the
// context of that repository. Note that this sets up arguments and environment
// variables for git, but doesn't run in the directory itself. If a directory
// is given, then the command will be run in that directory.
func (cf *ExecCommandFactory) newCommand(ctx context.Context, repo repository.GitRepo, dir string, globals []GlobalOption, sc Cmd, opts ...CmdOpt) (*command.Command, error) {
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
		return fmt.Errorf("subcommand %q: %w", sc.Subcommand(), ErrHookPayloadRequired)
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
	// 1. Globals which get set up by default for all git commands.
	// 2. Globals which get set up by default for a given git command.
	// 3. Globals passed via command options, e.g. as set up by
	//    `WithReftxHook()`.
	// 4. Globals passed directly to the command at the site of execution.
	var combinedGlobals []GlobalOption
	combinedGlobals = append(combinedGlobals, globalOptions...)
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
