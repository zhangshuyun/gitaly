package git_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestFlagValidation(t *testing.T) {
	for _, tt := range []struct {
		option git.Option
		valid  bool
	}{
		// valid Flag inputs
		{option: git.Flag{Name: "-k"}, valid: true},
		{option: git.Flag{Name: "-K"}, valid: true},
		{option: git.Flag{Name: "--asdf"}, valid: true},
		{option: git.Flag{Name: "--asdf-qwer"}, valid: true},
		{option: git.Flag{Name: "--asdf=qwerty"}, valid: true},
		{option: git.Flag{Name: "-D=A"}, valid: true},
		{option: git.Flag{Name: "-D="}, valid: true},

		// valid ValueFlag inputs
		{option: git.ValueFlag{"-k", "adsf"}, valid: true},
		{option: git.ValueFlag{"-k", "--anything"}, valid: true},
		{option: git.ValueFlag{"-k", ""}, valid: true},

		// valid SubSubCmd inputs
		{option: git.SubSubCmd{"meow"}, valid: true},

		// valid ConfigPair inputs
		{option: git.ConfigPair{Key: "a.b.c", Value: "d"}, valid: true},
		{option: git.ConfigPair{Key: "core.sound", Value: "meow"}, valid: true},
		{option: git.ConfigPair{Key: "asdf-qwer.1234-5678", Value: ""}, valid: true},
		{option: git.ConfigPair{Key: "http.https://user@example.com/repo.git.user", Value: "kitty"}, valid: true},

		// invalid Flag inputs
		{option: git.Flag{Name: "-*"}},          // invalid character
		{option: git.Flag{Name: "a"}},           // missing dash
		{option: git.Flag{Name: "[["}},          // suspicious characters
		{option: git.Flag{Name: "||"}},          // suspicious characters
		{option: git.Flag{Name: "asdf=qwerty"}}, // missing dash

		// invalid ValueFlag inputs
		{option: git.ValueFlag{"k", "asdf"}}, // missing dash

		// invalid SubSubCmd inputs
		{option: git.SubSubCmd{"--meow"}}, // cannot start with dash

		// invalid ConfigPair inputs
		{option: git.ConfigPair{Key: "", Value: ""}},            // key cannot be empty
		{option: git.ConfigPair{Key: " ", Value: ""}},           // key cannot be whitespace
		{option: git.ConfigPair{Key: "asdf", Value: ""}},        // two components required
		{option: git.ConfigPair{Key: "asdf.", Value: ""}},       // 2nd component must be non-empty
		{option: git.ConfigPair{Key: "--asdf.asdf", Value: ""}}, // key cannot start with dash
		{option: git.ConfigPair{Key: "as[[df.asdf", Value: ""}}, // 1st component cannot contain non-alphanumeric
		{option: git.ConfigPair{Key: "asdf.as]]df", Value: ""}}, // 2nd component cannot contain non-alphanumeric
	} {
		args, err := tt.option.ValidateArgs()
		if tt.valid {
			require.NoError(t, err)
		} else {
			require.Error(t, err,
				"expected error, but args %v passed validation", args)
			require.True(t, git.IsInvalidArgErr(err))
		}
	}
}

func TestSafeCmdInvalidArg(t *testing.T) {
	for _, tt := range []struct {
		globals []git.Option
		subCmd  git.SubCmd
		errMsg  string
	}{
		{
			subCmd: git.SubCmd{Name: "--meow"},
			errMsg: `invalid sub command name "--meow": invalid argument`,
		},
		{
			subCmd: git.SubCmd{
				Name:  "meow",
				Flags: []git.Option{git.Flag{Name: "woof"}},
			},
			errMsg: `flag "woof" failed regex validation: invalid argument`,
		},
		{
			subCmd: git.SubCmd{
				Name: "meow",
				Args: []string{"--tweet"},
			},
			errMsg: `positional arg "--tweet" cannot start with dash '-': invalid argument`,
		},
		{
			subCmd: git.SubCmd{
				Name:  "meow",
				Flags: []git.Option{git.SubSubCmd{"-invalid"}},
			},
			errMsg: `invalid sub-sub command name "-invalid": invalid argument`,
		},
	} {
		_, err := git.SafeCmd(
			context.Background(),
			&gitalypb.Repository{},
			tt.globals,
			tt.subCmd,
			git.WithRefTxHook(context.Background(), &gitalypb.Repository{}, config.Config),
		)
		require.EqualError(t, err, tt.errMsg)
		require.True(t, git.IsInvalidArgErr(err))
	}
}

func TestSafeCmdValid(t *testing.T) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	reenableGitCmd := disableGitCmd()
	defer reenableGitCmd()

	endOfOptions := "--end-of-options"

	for _, tt := range []struct {
		globals    []git.Option
		subCmd     git.SubCmd
		expectArgs []string
	}{
		{
			subCmd:     git.SubCmd{Name: "meow"},
			expectArgs: []string{"meow", endOfOptions},
		},
		{
			globals: []git.Option{
				git.Flag{Name: "--aaaa-bbbb"},
			},
			subCmd:     git.SubCmd{Name: "cccc"},
			expectArgs: []string{"--aaaa-bbbb", "cccc", endOfOptions},
		},
		{
			subCmd: git.SubCmd{
				Name:        "meow",
				Args:        []string{""},
				PostSepArgs: []string{"-woof", ""},
			},
			expectArgs: []string{"meow", "", endOfOptions, "--", "-woof", ""},
		},
		{
			globals: []git.Option{
				git.Flag{Name: "-a"},
				git.ValueFlag{"-b", "c"},
			},
			subCmd: git.SubCmd{
				Name: "d",
				Flags: []git.Option{
					git.Flag{Name: "-e"},
					git.ValueFlag{"-f", "g"},
					git.Flag{Name: "-h=i"},
				},
				Args:        []string{"1", "2"},
				PostSepArgs: []string{"3", "4", "5"},
			},
			expectArgs: []string{"-a", "-b", "c", "d", "-e", "-f", "g", "-h=i", "1", "2", endOfOptions, "--", "3", "4", "5"},
		},
		{
			subCmd: git.SubCmd{
				Name: "noun",
				Flags: []git.Option{
					git.SubSubCmd{"verb"},
					git.OutputToStdout,
					git.Flag{Name: "--adjective"},
				},
			},
			expectArgs: []string{"noun", "verb", "-", "--adjective", endOfOptions},
		},
		{
			globals: []git.Option{
				git.Flag{Name: "--contributing"},
				git.ValueFlag{"--author", "a-gopher"},
			},
			subCmd: git.SubCmd{
				Name: "accept",
				Args: []string{"mr"},
				Flags: []git.Option{
					git.Flag{Name: "--is-important"},
					git.ValueFlag{"--why", "looking-for-first-contribution"},
				},
			},
			expectArgs: []string{"--contributing", "--author", "a-gopher", "accept", "--is-important", "--why", "looking-for-first-contribution", "mr", endOfOptions},
		},
	} {
		opts := []git.CmdOpt{git.WithRefTxHook(ctx, &gitalypb.Repository{}, config.Config)}
		cmd, err := git.SafeCmd(ctx, testRepo, tt.globals, tt.subCmd, opts...)
		require.NoError(t, err)
		// ignore first 3 indeterministic args (executable path and repo args)
		require.Equal(t, tt.expectArgs, cmd.Args()[3:])

		cmd, err = git.SafeCmdWithEnv(ctx, nil, testRepo, tt.globals, tt.subCmd, opts...)
		require.NoError(t, err)
		// ignore first 3 indeterministic args (executable path and repo args)
		require.Equal(t, tt.expectArgs, cmd.Args()[3:])

		cmd, err = git.SafeStdinCmd(ctx, testRepo, tt.globals, tt.subCmd, opts...)
		require.NoError(t, err)
		require.Equal(t, tt.expectArgs, cmd.Args()[3:])

		cmd, err = git.SafeBareCmd(ctx, git.CmdStream{}, nil, tt.globals, tt.subCmd, opts...)
		require.NoError(t, err)
		// ignore first indeterministic arg (executable path)
		require.Equal(t, tt.expectArgs, cmd.Args()[1:])

		cmd, err = git.SafeCmdWithoutRepo(ctx, git.CmdStream{}, tt.globals, tt.subCmd)
		require.NoError(t, err)
		require.Equal(t, tt.expectArgs, cmd.Args()[1:])
	}
}

func TestSafeCmdWithEnv(t *testing.T) {
	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	reenableGitCmd := disableGitCmd()
	defer reenableGitCmd()

	globals := []git.Option{
		git.Flag{Name: "--aaaa-bbbb"},
	}

	subCmd := git.SubCmd{Name: "cccc"}
	endOfOptions := "--end-of-options"
	expectArgs := []string{"--aaaa-bbbb", "cccc", endOfOptions}

	env := []string{"TEST_VAR1=1", "TEST_VAR2=2"}

	opts := []git.CmdOpt{git.WithRefTxHook(ctx, &gitalypb.Repository{}, config.Config)}
	cmd, err := git.SafeCmdWithEnv(ctx, env, testRepo, globals, subCmd, opts...)
	require.NoError(t, err)
	// ignore first 3 indeterministic args (executable path and repo args)
	require.Equal(t, expectArgs, cmd.Args()[3:])
	require.Subset(t, cmd.Env(), env)
}

func disableGitCmd() testhelper.Cleanup {
	oldBinPath := config.Config.Git.BinPath
	config.Config.Git.BinPath = "/bin/echo"
	return func() { config.Config.Git.BinPath = oldBinPath }
}

func TestSafeBareCmdInDir(t *testing.T) {
	t.Run("no dir specified", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		_, err := git.SafeBareCmdInDir(ctx, "", git.CmdStream{}, nil, nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no 'dir' provided")
	})

	t.Run("runs in dir", func(t *testing.T) {
		_, repoPath, cleanup := testhelper.NewTestRepoWithWorktree(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		var stderr bytes.Buffer
		cmd, err := git.SafeBareCmdInDir(ctx, repoPath, git.CmdStream{Err: &stderr}, nil, nil, git.SubCmd{
			Name: "rev-parse",
			Args: []string{"master"},
		})
		require.NoError(t, err)

		revData, err := ioutil.ReadAll(cmd)
		require.NoError(t, err)

		require.NoError(t, cmd.Wait(), stderr.String())

		require.Equal(t, "1e292f8fedd741b75372e19097c76d327140c312", text.ChompBytes(revData))
	})

	t.Run("doesn't runs in non existing dir", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		var stderr bytes.Buffer
		_, err := git.SafeBareCmdInDir(ctx, "non-existing-dir", git.CmdStream{Err: &stderr}, nil, nil, git.SubCmd{
			Name: "rev-parse",
			Args: []string{"master"},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "no such file or directory")
	})
}

func TestSafeCmd(t *testing.T) {
	t.Run("stderr is empty if no error", func(t *testing.T) {
		repo, _, cleanup := testhelper.NewTestRepoWithWorktree(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		var stderr bytes.Buffer
		cmd, err := git.SafeCmd(ctx, repo, nil,
			git.SubCmd{
				Name: "rev-parse",
				Args: []string{"master"},
			},
			git.WithStderr(&stderr),
		)
		require.NoError(t, err)

		revData, err := ioutil.ReadAll(cmd)
		require.NoError(t, err)

		require.NoError(t, cmd.Wait(), stderr.String())

		require.Equal(t, "1e292f8fedd741b75372e19097c76d327140c312", text.ChompBytes(revData))
		require.Empty(t, stderr.String())
	})

	t.Run("stderr contains error message on failure", func(t *testing.T) {
		repo, _, cleanup := testhelper.NewTestRepoWithWorktree(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		var stderr bytes.Buffer
		cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{
			Name: "rev-parse",
			Args: []string{"invalid-ref"},
		},
			git.WithStderr(&stderr),
		)
		require.NoError(t, err)

		revData, err := ioutil.ReadAll(cmd)
		require.NoError(t, err)

		require.Error(t, cmd.Wait())

		require.Equal(t, "invalid-ref", text.ChompBytes(revData))
		require.Contains(t, stderr.String(), "fatal: ambiguous argument 'invalid-ref': unknown revision or path not in the working tree.")
	})
}
