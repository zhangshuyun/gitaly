package git

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
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

func TestWithConfig(t *testing.T) {
	var cfg config.Cfg
	require.NoError(t, cfg.SetGitPath())

	ctx, cancel := testhelper.Context()
	defer cancel()

	gitCmdFactory := NewExecCommandFactory(cfg)

	for _, tc := range []struct {
		desc           string
		configPairs    []ConfigPair
		expectedValues map[string]string
	}{
		{
			desc:        "no entries",
			configPairs: []ConfigPair{},
		},
		{
			desc: "single entry",
			configPairs: []ConfigPair{
				ConfigPair{Key: "foo.bar", Value: "baz"},
			},
			expectedValues: map[string]string{
				"foo.bar": "baz",
			},
		},
		{
			desc: "multiple entries",
			configPairs: []ConfigPair{
				ConfigPair{Key: "entry.one", Value: "1"},
				ConfigPair{Key: "entry.two", Value: "2"},
				ConfigPair{Key: "entry.three", Value: "3"},
			},
			expectedValues: map[string]string{
				"entry.one":   "1",
				"entry.two":   "2",
				"entry.three": "3",
			},
		},
		{
			desc: "later entries override previous ones",
			configPairs: []ConfigPair{
				ConfigPair{Key: "override.me", Value: "old value"},
				ConfigPair{Key: "unrelated.entry", Value: "unrelated value"},
				ConfigPair{Key: "override.me", Value: "new value"},
			},
			expectedValues: map[string]string{
				"unrelated.entry": "unrelated value",
				"override.me":     "new value",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			option := WithConfig(tc.configPairs...)

			var commandCfg cmdCfg
			require.NoError(t, option(&commandCfg))

			for expectedKey, expectedValue := range tc.expectedValues {
				var stdout bytes.Buffer
				configCmd, err := gitCmdFactory.NewWithoutRepo(ctx, SubCmd{
					Name: "config",
					Args: []string{expectedKey},
				}, WithStdout(&stdout), option)
				require.NoError(t, err)
				require.NoError(t, configCmd.Wait())
				require.Equal(t, expectedValue, text.ChompBytes(stdout.Bytes()))
			}
		})
	}
}

func TestWithConfigEnv(t *testing.T) {
	var cfg config.Cfg
	require.NoError(t, cfg.SetGitPath())

	ctx, cancel := testhelper.Context()
	defer cancel()

	gitCmdFactory := NewExecCommandFactory(cfg)

	version, err := CurrentVersion(ctx, gitCmdFactory)
	require.NoError(t, err)

	if !version.SupportsConfigEnv() {
		t.Skip("git does not support config env")
	}

	for _, tc := range []struct {
		desc           string
		configPairs    []ConfigPair
		expectedEnv    []string
		expectedValues map[string]string
	}{
		{
			desc:        "no entries",
			configPairs: []ConfigPair{},
			expectedEnv: []string{"GIT_CONFIG_COUNT=0"},
		},
		{
			desc: "single entry",
			configPairs: []ConfigPair{
				ConfigPair{Key: "foo.bar", Value: "baz"},
			},
			expectedEnv: []string{
				"GIT_CONFIG_KEY_0=foo.bar",
				"GIT_CONFIG_VALUE_0=baz",
				"GIT_CONFIG_COUNT=1",
			},
			expectedValues: map[string]string{
				"foo.bar": "baz",
			},
		},
		{
			desc: "multiple entries",
			configPairs: []ConfigPair{
				ConfigPair{Key: "entry.one", Value: "1"},
				ConfigPair{Key: "entry.two", Value: "2"},
				ConfigPair{Key: "entry.three", Value: "3"},
			},
			expectedEnv: []string{
				"GIT_CONFIG_KEY_0=entry.one",
				"GIT_CONFIG_VALUE_0=1",
				"GIT_CONFIG_KEY_1=entry.two",
				"GIT_CONFIG_VALUE_1=2",
				"GIT_CONFIG_KEY_2=entry.three",
				"GIT_CONFIG_VALUE_2=3",
				"GIT_CONFIG_COUNT=3",
			},
			expectedValues: map[string]string{
				"entry.one":   "1",
				"entry.two":   "2",
				"entry.three": "3",
			},
		},
		{
			desc: "later entries override previous ones",
			configPairs: []ConfigPair{
				ConfigPair{Key: "override.me", Value: "old value"},
				ConfigPair{Key: "unrelated.entry", Value: "unrelated value"},
				ConfigPair{Key: "override.me", Value: "new value"},
			},
			expectedEnv: []string{
				"GIT_CONFIG_KEY_0=override.me",
				"GIT_CONFIG_VALUE_0=old value",
				"GIT_CONFIG_KEY_1=unrelated.entry",
				"GIT_CONFIG_VALUE_1=unrelated value",
				"GIT_CONFIG_KEY_2=override.me",
				"GIT_CONFIG_VALUE_2=new value",
				"GIT_CONFIG_COUNT=3",
			},
			expectedValues: map[string]string{
				"unrelated.entry": "unrelated value",
				"override.me":     "new value",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			option := WithConfigEnv(tc.configPairs...)

			var commandCfg cmdCfg
			require.NoError(t, option(&commandCfg))
			require.EqualValues(t, tc.expectedEnv, commandCfg.env)

			for expectedKey, expectedValue := range tc.expectedValues {
				var stdout bytes.Buffer
				configCmd, err := gitCmdFactory.NewWithoutRepo(ctx, SubCmd{
					Name: "config",
					Args: []string{expectedKey},
				}, WithStdout(&stdout), option)
				require.NoError(t, err)
				require.NoError(t, configCmd.Wait())
				require.Equal(t, expectedValue, text.ChompBytes(stdout.Bytes()))
			}
		})
	}
}
