package gittest

import (
	"io"
	"os"
	"os/exec"
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// ExecConfig contains configuration for ExecOpts.
type ExecConfig struct {
	// Stdin sets up stdin of the spawned command.
	Stdin io.Reader
	// Stdout sets up stdout of the spawned command. Note that `ExecOpts()` will not return any
	// output anymore if this field is set.
	Stdout io.Writer
	// Env contains environment variables that should be appended to the spawned command's
	// environment.
	Env []string
}

// Exec runs a git command and returns the standard output, or fails.
func Exec(t testing.TB, cfg config.Cfg, args ...string) []byte {
	t.Helper()
	return ExecOpts(t, cfg, ExecConfig{}, args...)
}

// ExecOpts runs a git command with the given configuration.
func ExecOpts(t testing.TB, cfg config.Cfg, execCfg ExecConfig, args ...string) []byte {
	t.Helper()

	cmd := createCommand(t, cfg, execCfg, args...)

	output, err := cmd.Output()
	if err != nil {
		t.Log(cfg.Git.BinPath, args)
		if ee, ok := err.(*exec.ExitError); ok {
			t.Logf("%s: %s\n", ee.Stderr, output)
		}
		t.Fatal(err)
	}

	return output
}

// NewCommand creates a new Git command ready for execution.
func NewCommand(t testing.TB, cfg config.Cfg, args ...string) *exec.Cmd {
	t.Helper()
	return createCommand(t, cfg, ExecConfig{}, args...)
}

func createCommand(t testing.TB, cfg config.Cfg, execCfg ExecConfig, args ...string) *exec.Cmd {
	t.Helper()

	cmd := exec.Command(cfg.Git.BinPath, args...)
	cmd.Env = command.AllowedEnvironment(os.Environ())
	cmd.Env = append(command.GitEnv, cmd.Env...)
	cmd.Env = append(cmd.Env,
		"GIT_AUTHOR_DATE=1572776879 +0100",
		"GIT_COMMITTER_DATE=1572776879 +0100",
		"GIT_CONFIG_COUNT=4",
		"GIT_CONFIG_KEY_0=init.defaultBranch",
		"GIT_CONFIG_VALUE_0=master",
		"GIT_CONFIG_KEY_1=init.templateDir",
		"GIT_CONFIG_VALUE_1=",
		"GIT_CONFIG_KEY_2=user.name",
		"GIT_CONFIG_VALUE_2=Your Name",
		"GIT_CONFIG_KEY_3=user.email",
		"GIT_CONFIG_VALUE_3=you@example.com",
	)
	cmd.Env = append(cmd.Env, cfg.GitExecEnv()...)
	cmd.Env = append(cmd.Env, execCfg.Env...)

	cmd.Stdout = execCfg.Stdout
	cmd.Stdin = execCfg.Stdin

	return cmd
}
