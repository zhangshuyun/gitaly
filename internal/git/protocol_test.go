package git

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

type fakeProtocolMessage struct {
	protocol string
}

func (f fakeProtocolMessage) GetGitProtocol() string {
	return f.protocol
}

func TestGitProtocolEnv(t *testing.T) {
	for _, tt := range []struct {
		desc string
		msg  fakeProtocolMessage
		env  []string
	}{
		{
			desc: "no V2 request",
			env:  nil,
		},
		{
			desc: "V2 request",
			msg:  fakeProtocolMessage{protocol: "version=2"},
			env:  []string{"GIT_PROTOCOL=version=2"},
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			actual := gitProtocolEnv(ctx, tt.msg)
			require.Equal(t, tt.env, actual)
		})
	}
}
