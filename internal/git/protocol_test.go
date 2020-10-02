package git

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

type fakeProtocolMessage struct {
	protocol string
}

func (f fakeProtocolMessage) GetGitProtocol() string {
	return f.protocol
}

func TestAddGitProtocolEnv(t *testing.T) {
	env := []string{"fake=value"}

	for _, tt := range []struct {
		desc string
		msg  fakeProtocolMessage
		env  []string
	}{
		{
			desc: "no V2 request",
			env:  env,
		},
		{
			desc: "V2 request",
			msg:  fakeProtocolMessage{protocol: "version=2"},
			env:  append(env, "GIT_PROTOCOL=version=2"),
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			actual := AddGitProtocolEnv(ctx, tt.msg, env)
			require.Equal(t, tt.env, actual)
		})
	}
}
