package gittest

import (
	"context"
	"path/filepath"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

var _ git.CommandFactory = &InterceptingCommandFactory{}

// InterceptingCommandFactory is a git.CommandFactory which intercepts all executions of Git
// commands with a custom script.
type InterceptingCommandFactory struct {
	realCommandFactory         git.CommandFactory
	interceptingCommandFactory git.CommandFactory
}

// NewInterceptingCommandFactory creates a new command factory which intercepts Git commands. The
// given configuration must point to the real Git executable. The function will be executed to
// generate the script and receives as input the Git execution environment pointing to the real Git
// binary.
func NewInterceptingCommandFactory(
	ctx context.Context,
	tb testing.TB,
	cfg config.Cfg,
	generateScript func(git.ExecutionEnvironment) string,
) *InterceptingCommandFactory {
	// We create two different command factories. The first one will set up the execution
	// environment such that we can deterministically resolve the Git binary path as well as
	// required environment variables for both bundled and non-bundled Git. The second one will
	// then use a separate config which overrides the Git binary path to point to a custom
	// script supplied by the user.
	gitCmdFactory := git.NewExecCommandFactory(cfg)

	scriptPath := filepath.Join(testhelper.TempDir(tb), "git")
	testhelper.WriteExecutable(tb, scriptPath, []byte(generateScript(gitCmdFactory.GetExecutionEnvironment(ctx))))

	interceptingCfg := cfg
	interceptingCfg.Git.UseBundledBinaries = false
	interceptingCfg.Git.BinPath = scriptPath

	return &InterceptingCommandFactory{
		realCommandFactory:         gitCmdFactory,
		interceptingCommandFactory: git.NewExecCommandFactory(interceptingCfg),
	}
}

// New creates a new Git command for the given repository using the intercepting script.
func (f *InterceptingCommandFactory) New(ctx context.Context, repo repository.GitRepo, sc git.Cmd, opts ...git.CmdOpt) (*command.Command, error) {
	return f.interceptingCommandFactory.New(ctx, repo, sc, append(
		opts, git.WithEnv(f.realCommandFactory.GetExecutionEnvironment(ctx).EnvironmentVariables...),
	)...)
}

// NewWithoutRepo creates a new Git command using the intercepting script.
func (f *InterceptingCommandFactory) NewWithoutRepo(ctx context.Context, sc git.Cmd, opts ...git.CmdOpt) (*command.Command, error) {
	return f.interceptingCommandFactory.NewWithoutRepo(ctx, sc, append(
		opts, git.WithEnv(f.realCommandFactory.GetExecutionEnvironment(ctx).EnvironmentVariables...),
	)...)
}

// NewWithDir creates a new Git command in the given directory using the intercepting script.
func (f *InterceptingCommandFactory) NewWithDir(ctx context.Context, dir string, sc git.Cmd, opts ...git.CmdOpt) (*command.Command, error) {
	return f.interceptingCommandFactory.NewWithDir(ctx, dir, sc, append(
		opts, git.WithEnv(f.realCommandFactory.GetExecutionEnvironment(ctx).EnvironmentVariables...),
	)...)
}

// GetExecutionEnvironment returns the execution environment of the intercetping command factory.
// The Git binary path will point to the intercepting script, while environment variables will
// point to the intercepted Git installation.
func (f *InterceptingCommandFactory) GetExecutionEnvironment(ctx context.Context) git.ExecutionEnvironment {
	execEnv := f.realCommandFactory.GetExecutionEnvironment(ctx)
	execEnv.BinaryPath = f.interceptingCommandFactory.GetExecutionEnvironment(ctx).BinaryPath
	return execEnv
}
