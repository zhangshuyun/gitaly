package git

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddRemoteOpts_buildFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts RemoteAddOpts
		exp  []Option
	}{
		{
			desc: "none",
			exp:  nil,
		},
		{
			desc: "all set",
			opts: RemoteAddOpts{
				Tags:                   RemoteAddOptsTagsNone,
				Fetch:                  true,
				RemoteTrackingBranches: []string{"branch-1", "branch-2"},
				DefaultBranch:          "develop",
				Mirror:                 RemoteAddOptsMirrorPush,
			},
			exp: []Option{
				ValueFlag{Name: "-t", Value: "branch-1"},
				ValueFlag{Name: "-t", Value: "branch-2"},
				ValueFlag{Name: "-m", Value: "develop"},
				Flag{Name: "-f"},
				Flag{Name: "--no-tags"},
				ValueFlag{Name: "--mirror", Value: "push"},
			},
		},
		{
			desc: "with tags",
			opts: RemoteAddOpts{Tags: RemoteAddOptsTagsAll},
			exp:  []Option{Flag{Name: "--tags"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, tc.opts.buildFlags())
		})
	}
}

func TestRemoteSetURLOpts_buildFlags(t *testing.T) {
	for _, tc := range []struct {
		desc string
		opts SetURLOpts
		exp  []Option
	}{
		{
			desc: "none",
			exp:  nil,
		},
		{
			desc: "all set",
			opts: SetURLOpts{Push: true},
			exp:  []Option{Flag{Name: "--push"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.exp, tc.opts.buildFlags())
		})
	}
}
