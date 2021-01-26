package git

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigAddOpts_buildFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts ConfigAddOpts
		exp  []Option
	}{
		{
			desc: "none",
			opts: ConfigAddOpts{},
			exp:  nil,
		},
		{
			desc: "all set",
			opts: ConfigAddOpts{Type: ConfigTypeBoolOrInt},
			exp:  []Option{Flag{Name: "--bool-or-int"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, tc.opts.buildFlags())
		})
	}
}

func TestConfigGetRegexpOpts_buildFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts ConfigGetRegexpOpts
		exp  []Option
	}{
		{
			desc: "none",
			opts: ConfigGetRegexpOpts{},
			exp:  nil,
		},
		{
			desc: "all set",
			opts: ConfigGetRegexpOpts{Type: ConfigTypeInt, ShowOrigin: true, ShowScope: true},
			exp: []Option{
				Flag{Name: "--int"},
				Flag{Name: "--show-origin"},
				Flag{Name: "--show-scope"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, tc.opts.buildFlags())
		})
	}
}

func TestConfigUnsetOpts_buildFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts ConfigUnsetOpts
		exp  []Option
	}{
		{
			desc: "none",
			opts: ConfigUnsetOpts{},
			exp:  []Option{Flag{Name: "--unset"}},
		},
		{
			desc: "all set",
			opts: ConfigUnsetOpts{All: true, NotStrict: true},
			exp:  []Option{Flag{Name: "--unset-all"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, tc.opts.buildFlags())
		})
	}
}
