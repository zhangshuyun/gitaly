package git

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
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
