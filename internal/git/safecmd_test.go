package git_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
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
		{option: git.ConfigPair{"a.b.c", "d"}, valid: true},
		{option: git.ConfigPair{"core.sound", "meow"}, valid: true},
		{option: git.ConfigPair{"asdf-qwer.1234-5678", ""}, valid: true},
		{option: git.ConfigPair{"http.https://user@example.com/repo.git.user", "kitty"}, valid: true},

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
		{option: git.ConfigPair{"", ""}},            // key cannot be empty
		{option: git.ConfigPair{" ", ""}},           // key cannot be whitespace
		{option: git.ConfigPair{"asdf", ""}},        // two components required
		{option: git.ConfigPair{"asdf.", ""}},       // 2nd component must be non-empty
		{option: git.ConfigPair{"--asdf.asdf", ""}}, // key cannot start with dash
		{option: git.ConfigPair{"as[[df.asdf", ""}}, // 1st component cannot contain non-alphanumeric
		{option: git.ConfigPair{"asdf.as]]df", ""}}, // 2nd component cannot contain non-alphanumeric
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
		cmd, err := git.SafeCmd(ctx, testRepo, tt.globals, tt.subCmd)
		require.NoError(t, err)
		// ignore first 3 indeterministic args (executable path and repo args)
		require.Equal(t, tt.expectArgs, cmd.Args()[3:])

		cmd, err = git.SafeCmdWithEnv(ctx, nil, testRepo, tt.globals, tt.subCmd)
		require.NoError(t, err)
		// ignore first 3 indeterministic args (executable path and repo args)
		require.Equal(t, tt.expectArgs, cmd.Args()[3:])

		cmd, err = git.SafeStdinCmd(ctx, testRepo, tt.globals, tt.subCmd)
		require.NoError(t, err)
		require.Equal(t, tt.expectArgs, cmd.Args()[3:])

		cmd, err = git.SafeBareCmd(ctx, git.CmdStream{}, nil, tt.globals, tt.subCmd)
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

	cmd, err := git.SafeCmdWithEnv(ctx, env, testRepo, globals, subCmd)
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
