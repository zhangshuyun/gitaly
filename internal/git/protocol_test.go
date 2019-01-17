package git

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"gitlab.com/gitlab-org/gitaly/internal/config"
)

type fakeProtocolMessage struct {
	protocol string
}

func (f fakeProtocolMessage) GetGitProtocol() string {
	return f.protocol
}

func setGitProtocolV2Support(value bool) func() {
	orig := config.Config.Git.ProtocolV2Enabled
	config.Config.Git.ProtocolV2Enabled = value

	return func() {
		config.Config.Git.ProtocolV2Enabled = orig
	}
}

func TestAddGitProtocolEnvRespectsConfigEnabled(t *testing.T) {
	restore := setGitProtocolV2Support(true)
	defer restore()

	env := []string{"fake=value"}
	msg := fakeProtocolMessage{protocol: "version=2"}
	value := AddGitProtocolEnv(context.Background(), msg, env)

	require.Equal(t, value, append(env, "GIT_PROTOCOL=version=2"))
}

func TestAddGitProtocolEnvWhenV2NotRequested(t *testing.T) {
	restore := setGitProtocolV2Support(true)
	defer restore()

	env := []string{"fake=value"}
	msg := fakeProtocolMessage{protocol: ""}
	value := AddGitProtocolEnv(context.Background(), msg, env)
	require.Equal(t, value, env)
}

func TestAddGitProtocolEnvRespectsConfigDisabled(t *testing.T) {
	restore := setGitProtocolV2Support(false)
	defer restore()

	env := []string{"fake=value"}
	msg := fakeProtocolMessage{protocol: "GIT_PROTOCOL=version=2"}
	value := AddGitProtocolEnv(context.Background(), msg, env)
	require.Equal(t, value, env)
}
