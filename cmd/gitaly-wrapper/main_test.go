package main

import (
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap"
)

// TestStolenPid tests for regressions in https://gitlab.com/gitlab-org/gitaly/issues/1661
func TestStolenPid(t *testing.T) {
	defer func(oldValue string) {
		os.Setenv(bootstrap.PidFileEnvVar, oldValue)
	}(os.Getenv(bootstrap.PidFileEnvVar))

	pidFile, err := ioutil.TempFile("", "pidfile")
	require.NoError(t, err)
	defer os.Remove(pidFile.Name())

	os.Setenv(bootstrap.PidFileEnvVar, pidFile.Name())

	cmd := exec.Command("tail", "-f")
	require.NoError(t, cmd.Start())
	defer cmd.Process.Kill()

	_, err = pidFile.WriteString(strconv.Itoa(cmd.Process.Pid))
	require.NoError(t, err)
	require.NoError(t, pidFile.Close())

	tail, err := findGitaly()
	require.NoError(t, err)
	require.NotNil(t, tail)
	require.Equal(t, cmd.Process.Pid, tail.Pid)

	t.Run("stolen", func(t *testing.T) {
		require.False(t, isGitaly(tail, "/path/to/gitaly"))
	})

	t.Run("not stolen", func(t *testing.T) {
		require.True(t, isGitaly(tail, "/path/to/tail"))
	})
}
