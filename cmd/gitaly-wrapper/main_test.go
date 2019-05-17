package main

import (
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// TestStolenPid tests for regressions in https://gitlab.com/gitlab-org/gitaly/issues/1661
func TestStolenPid(t *testing.T) {
	defer func(oldValue string) {
		os.Setenv(config.EnvPidFile, oldValue)
	}(os.Getenv(config.EnvPidFile))

	pidFile, err := ioutil.TempFile("", "pidfile")
	require.NoError(t, err)
	defer os.Remove(pidFile.Name())

	os.Setenv(config.EnvPidFile, pidFile.Name())

	cmd := exec.Command("tail", "-f")
	require.NoError(t, cmd.Start())
	defer cmd.Process.Kill()

	_, err = pidFile.WriteString(strconv.Itoa(cmd.Process.Pid))
	require.NoError(t, err)
	require.NoError(t, pidFile.Close())

	gitaly, err := findGitaly()
	require.NoError(t, err)
	require.Nil(t, gitaly)
}

func TestExistingGitaly(t *testing.T) {
	defer func(oldValue string) {
		os.Setenv(config.EnvPidFile, oldValue)
	}(os.Getenv(config.EnvPidFile))

	tmpDir, err := ioutil.TempDir("", "gitaly-pid")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	pidFile := path.Join(tmpDir, "gitaly.pid")
	fakeGitaly := path.Join(tmpDir, "gitaly")

	require.NoError(t, buildFakeGitaly(t, fakeGitaly), "Can't build a fake gitaly binary")

	os.Setenv(config.EnvPidFile, pidFile)

	cmd := exec.Command(fakeGitaly, "-f")
	require.NoError(t, cmd.Start())
	defer cmd.Process.Kill()

	require.NoError(t, ioutil.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0644))

	gitaly, err := findGitaly()
	require.NoError(t, err)
	require.NotNil(t, gitaly)
	require.Equal(t, cmd.Process.Pid, gitaly.Pid)
	gitaly.Kill()
}

func buildFakeGitaly(t *testing.T, path string) error {
	tail := exec.Command("tail", "-f")
	require.NoError(t, tail.Start())
	defer tail.Process.Kill()

	tailPath, err := procPath(tail.Process.Pid)
	require.NoError(t, err)
	tail.Process.Kill()

	src, err := os.Open(tailPath)
	require.NoError(t, err)
	defer src.Close()

	out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0744)
	require.NoError(t, err)
	defer out.Close()

	if _, err := io.Copy(out, src); err != nil {
		return err
	}

	return out.Sync()
}
