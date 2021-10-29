package gittest

import (
	"io"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// Exec runs a git command and returns the standard output, or fails.
func Exec(t testing.TB, cfg config.Cfg, args ...string) []byte {
	t.Helper()

	return run(t, nil, nil, cfg, args, nil)
}

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

// ExecOpts runs a git command with the given configuration.
func ExecOpts(t testing.TB, cfg config.Cfg, execCfg ExecConfig, args ...string) []byte {
	t.Helper()

	return run(t, execCfg.Stdin, execCfg.Stdout, cfg, args, execCfg.Env)
}

func run(t testing.TB, stdin io.Reader, stdout io.Writer, cfg config.Cfg, args, env []string) []byte {
	t.Helper()

	cmd := exec.Command(cfg.Git.BinPath, args...)
	cmd.Env = os.Environ()
	cmd.Env = append(command.GitEnv, cmd.Env...)
	cmd.Env = append(cmd.Env,
		"GIT_AUTHOR_DATE=1572776879 +0100",
		"GIT_COMMITTER_DATE=1572776879 +0100",
		"GIT_CONFIG_COUNT=1",
		"GIT_CONFIG_KEY_0=init.defaultBranch",
		"GIT_CONFIG_VALUE_0=master",
	)
	cmd.Env = append(cmd.Env, env...)

	cmd.Stdout = stdout
	cmd.Stdin = stdin

	if stdout != nil {
		require.NoError(t, cmd.Start())
		require.NoError(t, cmd.Wait())

		return nil
	}

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
