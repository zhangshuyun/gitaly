package gittest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

// NewProtocolDetectingCommandFactory creates a new intercepting Git command factory that allows the
// protocol to be tested. It returns this factory and a function to read the GIT_PROTOCOL
// environment variable created by the wrapper script.
func NewProtocolDetectingCommandFactory(ctx context.Context, t testing.TB, cfg config.Cfg) (git.CommandFactory, func() string) {
	envPath := filepath.Join(testhelper.TempDir(t), "git-env")

	gitCmdFactory := NewInterceptingCommandFactory(ctx, t, cfg, func(execEnv git.ExecutionEnvironment) string {
		return fmt.Sprintf(
			`#!/usr/bin/env bash
			env | grep ^GIT_PROTOCOL= >>%q
			exec %q "$@"
		`, envPath, execEnv.BinaryPath)
	})

	return gitCmdFactory, func() string {
		data, err := os.ReadFile(envPath)
		require.NoError(t, err)
		return string(data)
	}
}
