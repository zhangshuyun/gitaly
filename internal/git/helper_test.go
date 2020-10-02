package git

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(m.Run())
}

func TestValidateRevision(t *testing.T) {
	testCases := []struct {
		rev string
		ok  bool
	}{
		{rev: "foo/bar", ok: true},
		{rev: "-foo/bar", ok: false},
		{rev: "foo bar", ok: false},
		{rev: "foo\x00bar", ok: false},
		{rev: "foo/bar:baz", ok: false},
	}

	for _, tc := range testCases {
		t.Run(tc.rev, func(t *testing.T) {
			err := ValidateRevision([]byte(tc.rev))
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestSupportsDeltaIslands(t *testing.T) {
	testCases := []struct {
		version string
		fail    bool
		delta   bool
	}{
		{version: "2.20.0", delta: true},
		{version: "2.21.5", delta: true},
		{version: "2.19.8", delta: false},
		{version: "1.20.8", delta: false},
		{version: "1.18.0", delta: false},
		{version: "2.28.0.rc0", delta: true},
		{version: "2.20", fail: true},
		{version: "bla bla", fail: true},
	}

	for _, tc := range testCases {
		t.Run(tc.version, func(t *testing.T) {
			out, err := SupportsDeltaIslands(tc.version)

			if tc.fail {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.delta, out, "delta island support")
		})
	}
}
