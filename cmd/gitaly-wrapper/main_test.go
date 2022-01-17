package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

// TestStolenPid tests for regressions in https://gitlab.com/gitlab-org/gitaly/issues/1661
func TestStolenPid(t *testing.T) {
	t.Parallel()

	tempDir := testhelper.TempDir(t)

	pidFile, err := os.Create(filepath.Join(tempDir, "pidfile"))
	require.NoError(t, err)

	ctx, cancel := testhelper.Context()
	defer cancel()

	cmd := exec.CommandContext(ctx, "tail", "-f")
	require.NoError(t, cmd.Start())
	defer func() {
		cancel()
		_ = cmd.Wait()
	}()

	_, err = pidFile.WriteString(strconv.Itoa(cmd.Process.Pid))
	require.NoError(t, err)
	require.NoError(t, pidFile.Close())

	tail, err := findProcess(pidFile.Name())
	require.NoError(t, err)
	require.NotNil(t, tail)
	require.Equal(t, cmd.Process.Pid, tail.Pid)

	t.Run("stolen", func(t *testing.T) {
		require.False(t, isExpectedProcess(tail, "/path/to/gitaly"))
	})

	t.Run("not stolen", func(t *testing.T) {
		require.True(t, isExpectedProcess(tail, "/path/to/tail"))
	})
}

func TestFindProcess(t *testing.T) {
	t.Parallel()

	t.Run("nonexistent PID file", func(t *testing.T) {
		t.Parallel()

		_, err := findProcess("does-not-exist")
		require.True(t, os.IsNotExist(err))
	})

	t.Run("PID file with garbage", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(testhelper.TempDir(t), "pid")
		require.NoError(t, os.WriteFile(path, []byte("garbage"), 0o644))

		_, err := findProcess(path)
		_, expectedErr := strconv.Atoi("garbage")
		require.Equal(t, expectedErr, err)
	})

	t.Run("nonexistent process", func(t *testing.T) {
		t.Parallel()

		// The below PID can exist, but chances are sufficiently low to hopefully not matter
		// in practice.
		path := filepath.Join(testhelper.TempDir(t), "pid")
		require.NoError(t, os.WriteFile(path, []byte("7777777"), 0o644))

		// The process isn't alive, so we expect neither an error nor a process to be
		// returned.
		process, err := findProcess(path)
		require.Nil(t, process)
		require.Nil(t, err)
	})

	t.Run("running process", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := testhelper.Context()
		defer cancel()

		// The process will exit immediately, but that's not much of a problem given that
		// `findProcess()` also works with zombies.
		executable := testhelper.WriteExecutable(t, filepath.Join(testhelper.TempDir(t), "noop"), []byte(
			`#!/usr/bin/env bash
			echo ready
		`))

		cmd := exec.CommandContext(ctx, executable)
		stdout, err := cmd.StdoutPipe()
		require.NoError(t, err)

		require.NoError(t, cmd.Start())
		defer func() {
			require.NoError(t, cmd.Wait())
		}()

		// Wait for the process to be ready such that we know it's started up successfully
		// and is executing the bash shell.
		_, err = stdout.Read(make([]byte, 10))
		require.NoError(t, err)

		path := filepath.Join(testhelper.TempDir(t), "pid")
		require.NoError(t, os.WriteFile(path, []byte(strconv.FormatInt(int64(cmd.Process.Pid), 10)), 0o644))

		process, err := findProcess(path)
		require.NotNil(t, process)
		require.Equal(t, cmd.Process.Pid, process.Pid)
		require.Equal(t, nil, err)
	})
}

func TestIsRecoverable(t *testing.T) {
	t.Parallel()

	_, numericError := strconv.Atoi("")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "file doesn't exist",
			err:  os.ErrNotExist,
			want: true,
		},
		{
			name: "numeric error",
			err:  numericError,
			want: true,
		},
		{
			name: "generic error",
			err:  errors.New("generic error"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRecoverable(tt.err); got != tt.want {
				t.Errorf("isRecoverable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadPIDFile(t *testing.T) {
	t.Parallel()

	t.Run("nonexistent", func(t *testing.T) {
		t.Parallel()

		_, err := readPIDFile("does-not-exist")
		require.True(t, os.IsNotExist(err))
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(testhelper.TempDir(t), "pid")
		require.NoError(t, os.WriteFile(path, nil, 0o644))
		_, err := readPIDFile(path)
		_, expectedErr := strconv.Atoi("")
		require.Equal(t, expectedErr, err)
	})

	t.Run("invalid contents", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(testhelper.TempDir(t), "pid")
		require.NoError(t, os.WriteFile(path, []byte("invalid"), 0o644))
		_, err := readPIDFile(path)
		_, expectedErr := strconv.Atoi("invalid")
		require.Equal(t, expectedErr, err)
	})

	t.Run("valid", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(testhelper.TempDir(t), "pid")
		require.NoError(t, os.WriteFile(path, []byte("12345"), 0o644))
		pid, err := readPIDFile(path)
		require.NoError(t, err)
		require.Equal(t, 12345, pid)
	})
}

func TestIsExpectedProcess(t *testing.T) {
	t.Parallel()

	ctx, cancel := testhelper.Context()
	defer cancel()

	executable := testhelper.WriteExecutable(t, filepath.Join(testhelper.TempDir(t), "noop"), []byte(
		`#!/usr/bin/env bash
		echo ready
		read wait_until_killed
	`))

	cmd := exec.CommandContext(ctx, executable)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)

	require.NoError(t, cmd.Start())
	defer func() {
		testhelper.MustClose(t, stdin)
		require.Error(t, cmd.Wait())
	}()

	// Wait for the process to be ready such that we know it's started up successfully
	// and is executing the bash shell.
	_, err = stdout.Read(make([]byte, 10))
	require.NoError(t, err)

	require.False(t, isExpectedProcess(cmd.Process, "does not match"))
	require.True(t, isExpectedProcess(cmd.Process, "bash"))
}

func TestIsProcessAlive(t *testing.T) {
	t.Parallel()

	t.Run("nonexistent process", func(t *testing.T) {
		t.Parallel()

		// And now let's check with a nonexistent process. FindProcess never returns an
		// error on Unix systems even if the process doesn't exist, so this is fine.
		process, err := os.FindProcess(77777777)
		require.NoError(t, err)
		require.False(t, isProcessAlive(process))
	})

	t.Run("existing process", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := testhelper.Context()
		defer cancel()

		executable := testhelper.WriteExecutable(t, filepath.Join(testhelper.TempDir(t), "noop"), []byte(
			`#!/usr/bin/env bash
			echo ready
			read wait_until_killed
		`))

		cmd := exec.CommandContext(ctx, executable)
		stdout, err := cmd.StdoutPipe()
		require.NoError(t, err)
		_, err = cmd.StdinPipe()
		require.NoError(t, err)

		require.NoError(t, cmd.Start())

		// Wait for the process to be ready such that we know it's started up successfully
		// and is executing the bash shell.
		_, err = stdout.Read(make([]byte, 10))
		require.NoError(t, err)

		t.Run("running", func(t *testing.T) {
			require.True(t, isProcessAlive(cmd.Process))
		})

		t.Run("zombie", func(t *testing.T) {
			// The process will be considered alive as long as it hasn't been reaped yet.
			require.NoError(t, cmd.Process.Kill())
			require.True(t, isProcessAlive(cmd.Process))
		})

		t.Run("reaped", func(t *testing.T) {
			require.Error(t, cmd.Wait())
			require.False(t, isProcessAlive(cmd.Process))
		})
	})
}

func TestRun(t *testing.T) {
	t.Parallel()

	binary := testcfg.BuildGitalyWrapper(t, testcfg.Build(t))

	t.Run("missing arguments", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := testhelper.Context()
		defer cancel()

		output, err := exec.CommandContext(ctx, binary).CombinedOutput()
		require.Error(t, err)
		require.Contains(t, string(output), "usage: ")
	})

	t.Run("missing PID file envvar", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := testhelper.Context()
		defer cancel()

		output, err := exec.CommandContext(ctx, binary, "binary").CombinedOutput()
		require.Error(t, err)
		require.Contains(t, string(output), "missing pid file ENV variable")
	})

	t.Run("invalid executable", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := testhelper.Context()
		defer cancel()

		pidPath := filepath.Join(testhelper.TempDir(t), "pid")

		cmd := exec.CommandContext(ctx, binary, "does-not-exist")
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", bootstrap.EnvPidFile, pidPath))

		output, err := cmd.CombinedOutput()
		require.Error(t, err)
		require.Contains(t, string(output), "executable file not found in $PATH")
	})

	t.Run("adopting executable", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := testhelper.Context()
		defer cancel()

		script := testhelper.WriteExecutable(t, filepath.Join(testhelper.TempDir(t), "script"), []byte(
			`#!/usr/bin/env bash
			echo ready
			read wait_until_closed
		`))

		scriptCmd := exec.CommandContext(ctx, script)
		scriptStdout, err := scriptCmd.StdoutPipe()
		require.NoError(t, err)
		scriptStdin, err := scriptCmd.StdinPipe()
		require.NoError(t, err)

		require.NoError(t, scriptCmd.Start())

		// Read the first byte such that we know the process has been spawned.
		_, err = scriptStdout.Read(make([]byte, 10))
		require.NoError(t, err)

		// Write the PID of the running process into the PID file. As a result, it should
		// get adopted by gitaly-wrapper, which means it wouldn't try to execute it anew.
		pidPath := filepath.Join(testhelper.TempDir(t), "pid")
		require.NoError(t, os.WriteFile(pidPath, []byte(strconv.FormatInt(int64(scriptCmd.Process.Pid), 10)), 0o644))

		// Run gitaly-script with a binary path whose basename matches, but which ultimately
		// doesn't exist. This proves that it doesn't try to execute the script again.
		wrapperCmd := exec.CommandContext(ctx, binary, "/does/not/exist/bash")
		wrapperCmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", bootstrap.EnvPidFile, pidPath))
		wrapperStdout, err := wrapperCmd.StdoutPipe()
		require.NoError(t, err)
		require.NoError(t, wrapperCmd.Start())

		// We're now waiting for gitaly-wrapper to adopt the running script.
		reader := bufio.NewReader(wrapperStdout)
		for _, expectedLine := range []string{
			"Wrapper started",
			"finding process",
			"adopting a process",
		} {
			line, err := reader.ReadBytes('\n')
			require.NoError(t, err, "reading expected line %q",
				expectedLine)
			require.Contains(t, string(line), expectedLine)
		}

		// The script has been adopted, so we can now close its stdin and thus cause it to
		// exit.
		testhelper.MustClose(t, scriptStdin)
		require.Error(t, scriptCmd.Wait())

		// As a result, the wrapper should also exit successfully.
		require.NoError(t, wrapperCmd.Wait())
	})

	t.Run("spawning executable", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := testhelper.Context()
		defer cancel()

		script := testhelper.WriteExecutable(t, filepath.Join(testhelper.TempDir(t), "script"), []byte(
			`#!/usr/bin/env bash
			echo "I have been executed"
		`))

		pidPath := filepath.Join(testhelper.TempDir(t), "pid")

		cmd := exec.CommandContext(ctx, binary, script)
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", bootstrap.EnvPidFile, pidPath))
		output, err := cmd.CombinedOutput()
		require.NoError(t, err)

		require.Contains(t, string(output), "spawning a process")
		require.Contains(t, string(output), "I have been executed")

		require.NoFileExists(t, pidPath)
	})

	t.Run("spawning executable with missing process", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := testhelper.Context()
		defer cancel()

		script := testhelper.WriteExecutable(t, filepath.Join(testhelper.TempDir(t), "script"), []byte(
			`#!/usr/bin/env bash
			echo "I have been executed"
		`))

		pidPath := filepath.Join(testhelper.TempDir(t), "pid")
		require.NoError(t, os.WriteFile(pidPath, []byte("12345"), 0o644))

		cmd := exec.CommandContext(ctx, binary, script)
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", bootstrap.EnvPidFile, pidPath))

		output, err := cmd.CombinedOutput()
		require.NoError(t, err)
		require.Contains(t, string(output), "spawning a process")
		require.Contains(t, string(output), "I have been executed")
	})

	t.Run("spawning executable with zombie process", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := testhelper.Context()
		defer cancel()

		script := testhelper.WriteExecutable(t, filepath.Join(testhelper.TempDir(t), "script"), []byte(
			`#!/usr/bin/env bash
			echo "I have been executed"
		`))

		scriptCmd := exec.CommandContext(ctx, script)
		scriptStdout, err := scriptCmd.StdoutPipe()
		require.NoError(t, err)
		require.NoError(t, scriptCmd.Start())

		// Read until we get an EOF, which means that the script has terminated. It's now in
		// a zombie state because we don't call `Wait()`.
		_, err = io.ReadAll(scriptStdout)
		require.NoError(t, err)

		pidPath := filepath.Join(testhelper.TempDir(t), "pid")
		require.NoError(t, os.WriteFile(pidPath, []byte(strconv.FormatInt(int64(scriptCmd.Process.Pid), 10)), 0o644))

		cmd := exec.CommandContext(ctx, binary, script)
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", bootstrap.EnvPidFile, pidPath))

		output, err := cmd.CombinedOutput()
		require.NoError(t, err)
		require.Contains(t, string(output), "spawning a process")
		require.Contains(t, string(output), "I have been executed")

		require.NoError(t, scriptCmd.Wait())
	})
}
