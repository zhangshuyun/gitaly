package git

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
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

func newCommandFactory(tb testing.TB, cfg config.Cfg, opts ...ExecCommandFactoryOption) *ExecCommandFactory {
	gitCmdFactory, cleanup, err := NewExecCommandFactory(cfg, opts...)
	require.NoError(tb, err)
	tb.Cleanup(cleanup)
	return gitCmdFactory
}
