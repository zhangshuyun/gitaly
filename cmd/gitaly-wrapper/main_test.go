package main

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

// TestStolenPid tests for regressions in https://gitlab.com/gitlab-org/gitaly/issues/1661
func TestStolenPid(t *testing.T) {
	defer func(oldValue string) {
		require.NoError(t, os.Setenv(bootstrap.EnvPidFile, oldValue))
	}(os.Getenv(bootstrap.EnvPidFile))

	pidFile, err := os.CreateTemp("", "pidfile")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.Remove(pidFile.Name())) }()

	require.NoError(t, os.Setenv(bootstrap.EnvPidFile, pidFile.Name()))

	ctx, cancel := testhelper.Context()
	defer cancel()

	cmd := exec.CommandContext(ctx, "tail", "-f")
	require.NoError(t, cmd.Start())

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

func TestIsRecoverable(t *testing.T) {
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
