package git

import (
	"testing"

	"github.com/stretchr/testify/require"
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
