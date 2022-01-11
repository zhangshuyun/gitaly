package gittest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestInterceptingCommandFactory(t *testing.T) {
	cfg, repoProto, repoPath := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	factory := NewInterceptingCommandFactory(ctx, t, cfg, func(execEnv git.ExecutionEnvironment) string {
		return fmt.Sprintf(
			`#!/usr/bin/env bash
			%q rev-parse --sq-quote 'Hello, world!'
		`, execEnv.BinaryPath)
	})
	expectedString := " 'Hello, world'\\!''\n"

	t.Run("New", func(t *testing.T) {
		var stdout bytes.Buffer
		cmd, err := factory.New(ctx, repoProto, git.SubCmd{
			Name: "rev-parse",
			Args: []string{"something"},
		}, git.WithStdout(&stdout))
		require.NoError(t, err)
		require.NoError(t, cmd.Wait())
		require.Equal(t, expectedString, stdout.String())
	})

	t.Run("NewWithDir", func(t *testing.T) {
		var stdout bytes.Buffer
		cmd, err := factory.NewWithDir(ctx, repoPath, git.SubCmd{
			Name: "rev-parse",
			Args: []string{"something"},
		}, git.WithStdout(&stdout))
		require.NoError(t, err)
		require.NoError(t, cmd.Wait())
		require.Equal(t, expectedString, stdout.String())
	})

	t.Run("NewWithoutRepo", func(t *testing.T) {
		var stdout bytes.Buffer
		cmd, err := factory.NewWithoutRepo(ctx, git.SubCmd{
			Name: "rev-parse",
			Args: []string{"something"},
			Flags: []git.Option{
				git.ValueFlag{Name: "-C", Value: repoPath},
			},
		}, git.WithStdout(&stdout))
		require.NoError(t, err)
		require.NoError(t, cmd.Wait())
		require.Equal(t, expectedString, stdout.String())
	})
}
