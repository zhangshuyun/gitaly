package git

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestFlagValidation(t *testing.T) {
	for _, tt := range []struct {
		option Option
		valid  bool
	}{
		// valid Flag inputs
		{option: Flag{Name: "-k"}, valid: true},
		{option: Flag{Name: "-K"}, valid: true},
		{option: Flag{Name: "--asdf"}, valid: true},
		{option: Flag{Name: "--asdf-qwer"}, valid: true},
		{option: Flag{Name: "--asdf=qwerty"}, valid: true},
		{option: Flag{Name: "-D=A"}, valid: true},
		{option: Flag{Name: "-D="}, valid: true},

		// valid ValueFlag inputs
		{option: ValueFlag{"-k", "adsf"}, valid: true},
		{option: ValueFlag{"-k", "--anything"}, valid: true},
		{option: ValueFlag{"-k", ""}, valid: true},

		// valid ConfigPair inputs
		{option: ConfigPair{Key: "a.b.c", Value: "d"}, valid: true},
		{option: ConfigPair{Key: "core.sound", Value: "meow"}, valid: true},
		{option: ConfigPair{Key: "asdf-qwer.1234-5678", Value: ""}, valid: true},
		{option: ConfigPair{Key: "http.https://user@example.com/repo.git.user", Value: "kitty"}, valid: true},

		// invalid Flag inputs
		{option: Flag{Name: "-*"}},          // invalid character
		{option: Flag{Name: "a"}},           // missing dash
		{option: Flag{Name: "[["}},          // suspicious characters
		{option: Flag{Name: "||"}},          // suspicious characters
		{option: Flag{Name: "asdf=qwerty"}}, // missing dash

		// invalid ValueFlag inputs
		{option: ValueFlag{"k", "asdf"}}, // missing dash

		// invalid ConfigPair inputs
		{option: ConfigPair{Key: "", Value: ""}},            // key cannot be empty
		{option: ConfigPair{Key: " ", Value: ""}},           // key cannot be whitespace
		{option: ConfigPair{Key: "asdf", Value: ""}},        // two components required
		{option: ConfigPair{Key: "asdf.", Value: ""}},       // 2nd component must be non-empty
		{option: ConfigPair{Key: "--asdf.asdf", Value: ""}}, // key cannot start with dash
		{option: ConfigPair{Key: "as[[df.asdf", Value: ""}}, // 1st component cannot contain non-alphanumeric
		{option: ConfigPair{Key: "asdf.as]]df", Value: ""}}, // 2nd component cannot contain non-alphanumeric
	} {
		args, err := tt.option.OptionArgs()
		if tt.valid {
			require.NoError(t, err)
		} else {
			require.Error(t, err,
				"expected error, but args %v passed validation", args)
			require.True(t, IsInvalidArgErr(err))
		}
	}
}

func TestGlobalOption(t *testing.T) {
	for _, tc := range []struct {
		desc     string
		option   GlobalOption
		valid    bool
		expected []string
	}{
		{
			desc:     "single-letter flag",
			option:   Flag{Name: "-k"},
			valid:    true,
			expected: []string{"-k"},
		},
		{
			desc:     "long option flag",
			option:   Flag{Name: "--asdf"},
			valid:    true,
			expected: []string{"--asdf"},
		},
		{
			desc:     "multiple single-letter flags",
			option:   Flag{Name: "-abc"},
			valid:    true,
			expected: []string{"-abc"},
		},
		{
			desc:     "single-letter option with value",
			option:   Flag{Name: "-a=value"},
			valid:    true,
			expected: []string{"-a=value"},
		},
		{
			desc:     "long option with value",
			option:   Flag{Name: "--asdf=value"},
			valid:    true,
			expected: []string{"--asdf=value"},
		},
		{
			desc:   "flags without dashes are not allowed",
			option: Flag{Name: "foo"},
			valid:  false,
		},
		{
			desc:   "leading spaces are not allowed",
			option: Flag{Name: " -a"},
			valid:  false,
		},

		{
			desc:     "single-letter value flag",
			option:   ValueFlag{Name: "-a", Value: "value"},
			valid:    true,
			expected: []string{"-a", "value"},
		},
		{
			desc:     "long option value flag",
			option:   ValueFlag{Name: "--foobar", Value: "value"},
			valid:    true,
			expected: []string{"--foobar", "value"},
		},
		{
			desc:     "multiple single-letters for value flag",
			option:   ValueFlag{Name: "-abc", Value: "value"},
			valid:    true,
			expected: []string{"-abc", "value"},
		},
		{
			desc:     "value flag with empty value",
			option:   ValueFlag{Name: "--key", Value: ""},
			valid:    true,
			expected: []string{"--key", ""},
		},
		{
			desc:   "value flag without dashes are not allowed",
			option: ValueFlag{Name: "foo", Value: "bar"},
			valid:  false,
		},
		{
			desc:   "value flag with empty key are not allowed",
			option: ValueFlag{Name: "", Value: "bar"},
			valid:  false,
		},

		{
			desc:     "config pair with key and value",
			option:   ConfigPair{Key: "foo.bar", Value: "value"},
			valid:    true,
			expected: []string{"-c", "foo.bar=value"},
		},
		{
			desc:     "config pair with subsection",
			option:   ConfigPair{Key: "foo.bar.baz", Value: "value"},
			valid:    true,
			expected: []string{"-c", "foo.bar.baz=value"},
		},
		{
			desc:     "config pair without value",
			option:   ConfigPair{Key: "foo.bar"},
			valid:    true,
			expected: []string{"-c", "foo.bar="},
		},
		{
			desc:   "config pair with invalid section format",
			option: ConfigPair{Key: "foo", Value: "value"},
			valid:  false,
		},
		{
			desc:   "config pair with leading whitespace",
			option: ConfigPair{Key: " foo.bar", Value: "value"},
			valid:  false,
		},
		{
			desc:   "config pair with disallowed character in key",
			option: ConfigPair{Key: "http.https://weak.example.com.sslVerify", Value: "false"},
			valid:  false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			args, err := tc.option.GlobalArgs()
			if tc.valid {
				require.NoError(t, err)
				require.Equal(t, tc.expected, args)
			} else {
				require.Error(t, err, "expected error, but args %v passed validation", args)
				require.True(t, IsInvalidArgErr(err))
			}
		})
	}
}

func TestNewCommandInvalidArg(t *testing.T) {
	for _, tt := range []struct {
		globals []GlobalOption
		subCmd  Cmd
		errMsg  string
	}{
		{
			subCmd: SubCmd{Name: "--meow"},
			errMsg: `invalid sub command name "--meow": invalid argument`,
		},
		{
			subCmd: SubCmd{
				Name:  "update-ref",
				Flags: []Option{Flag{Name: "woof"}},
			},
			errMsg: `flag "woof" failed regex validation: invalid argument`,
		},
		{
			subCmd: SubCmd{
				Name: "update-ref",
				Args: []string{"--tweet"},
			},
			errMsg: `positional arg "--tweet" cannot start with dash '-': invalid argument`,
		},
		{
			subCmd: SubSubCmd{
				Name:   "update-ref",
				Action: "-invalid",
			},
			errMsg: `invalid sub command action "-invalid": invalid argument`,
		},
	} {
		_, err := NewCommand(
			context.Background(),
			&gitalypb.Repository{},
			tt.globals,
			tt.subCmd,
			WithRefTxHook(context.Background(), &gitalypb.Repository{}, config.Config),
		)
		require.EqualError(t, err, tt.errMsg)
		require.True(t, IsInvalidArgErr(err))
	}
}

func TestNewCommandValid(t *testing.T) {
	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	reenableGitCmd := disableGitCmd()
	defer reenableGitCmd()

	cfg := config.Config
	hooksPath := "core.hooksPath=" + hooks.Path(cfg)
	gitCmdFactory := NewExecCommandFactory(cfg)

	endOfOptions := "--end-of-options"

	for _, tt := range []struct {
		desc       string
		globals    []GlobalOption
		subCmd     Cmd
		expectArgs []string
	}{
		{
			desc:       "no args",
			subCmd:     SubCmd{Name: "update-ref"},
			expectArgs: []string{"-c", hooksPath, "update-ref", endOfOptions},
		},
		{
			desc: "single option",
			globals: []GlobalOption{
				Flag{Name: "--aaaa-bbbb"},
			},
			subCmd:     SubCmd{Name: "update-ref"},
			expectArgs: []string{"-c", hooksPath, "--aaaa-bbbb", "update-ref", endOfOptions},
		},
		{
			desc: "empty arg and postsep args",
			subCmd: SubCmd{
				Name:        "update-ref",
				Args:        []string{""},
				PostSepArgs: []string{"-woof", ""},
			},
			expectArgs: []string{"-c", hooksPath, "update-ref", "", endOfOptions, "--", "-woof", ""},
		},
		{
			desc: "full blown",
			globals: []GlobalOption{
				Flag{Name: "-a"},
				ValueFlag{"-b", "c"},
			},
			subCmd: SubCmd{
				Name: "update-ref",
				Flags: []Option{
					Flag{Name: "-e"},
					ValueFlag{"-f", "g"},
					Flag{Name: "-h=i"},
				},
				Args:        []string{"1", "2"},
				PostSepArgs: []string{"3", "4", "5"},
			},
			expectArgs: []string{"-c", hooksPath, "-a", "-b", "c", "update-ref", "-e", "-f", "g", "-h=i", "1", "2", endOfOptions, "--", "3", "4", "5"},
		},
		{
			desc: "output to stdout",
			subCmd: SubSubCmd{
				Name:   "update-ref",
				Action: "verb",
				Flags: []Option{
					OutputToStdout,
					Flag{Name: "--adjective"},
				},
			},
			expectArgs: []string{"-c", hooksPath, "update-ref", "verb", "-", "--adjective", endOfOptions},
		},
		{
			desc: "multiple value flags",
			globals: []GlobalOption{
				Flag{Name: "--contributing"},
				ValueFlag{"--author", "a-gopher"},
			},
			subCmd: SubCmd{
				Name: "update-ref",
				Args: []string{"mr"},
				Flags: []Option{
					Flag{Name: "--is-important"},
					ValueFlag{"--why", "looking-for-first-contribution"},
				},
			},
			expectArgs: []string{"-c", hooksPath, "--contributing", "--author", "a-gopher", "update-ref", "--is-important", "--why", "looking-for-first-contribution", "mr", endOfOptions},
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			opts := []CmdOpt{WithRefTxHook(ctx, &gitalypb.Repository{}, cfg)}

			cmd, err := NewCommand(ctx, testRepo, tt.globals, tt.subCmd, opts...)
			require.NoError(t, err)
			// ignore first 3 indeterministic args (executable path and repo args)
			require.Equal(t, tt.expectArgs, cmd.Args()[3:])

			cmd, err = NewCommandWithoutRepo(ctx, tt.globals, tt.subCmd, opts...)
			require.NoError(t, err)
			// ignore first indeterministic arg (executable path)
			require.Equal(t, tt.expectArgs, cmd.Args()[1:])

			cmd, err = gitCmdFactory.NewWithDir(ctx, testRepoPath, tt.globals, tt.subCmd, opts...)
			require.NoError(t, err)
			require.Equal(t, tt.expectArgs, cmd.Args()[1:])
		})
	}
}

func disableGitCmd() testhelper.Cleanup {
	oldBinPath := config.Config.Git.BinPath
	config.Config.Git.BinPath = "/bin/echo"
	return func() { config.Config.Git.BinPath = oldBinPath }
}

func TestNewCommand(t *testing.T) {
	t.Run("stderr is empty if no error", func(t *testing.T) {
		repo, _, cleanup := testhelper.NewTestRepoWithWorktree(t)
		defer cleanup()

		ctx, cancel := testhelper.Context()
		defer cancel()

		var stderr bytes.Buffer
		cmd, err := NewCommand(ctx, repo, nil,
			SubCmd{
				Name: "rev-parse",
				Args: []string{"master"},
			},
			WithStderr(&stderr),
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
		cmd, err := NewCommand(ctx, repo, nil, SubCmd{
			Name: "rev-parse",
			Args: []string{"invalid-ref"},
		},
			WithStderr(&stderr),
		)
		require.NoError(t, err)

		revData, err := ioutil.ReadAll(cmd)
		require.NoError(t, err)

		require.Error(t, cmd.Wait())

		require.Equal(t, "invalid-ref", text.ChompBytes(revData))
		require.Contains(t, stderr.String(), "fatal: ambiguous argument 'invalid-ref': unknown revision or path not in the working tree.")
	})
}
