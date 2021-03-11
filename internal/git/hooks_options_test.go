package git_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
)

func TestWithRefHook(t *testing.T) {
	cfg, repo, _, cleanup := testcfg.BuildWithRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	opt := git.WithRefTxHook(ctx, repo, cfg)
	subCmd := git.SubCmd{Name: "update-ref", Args: []string{"refs/heads/master", git.ZeroOID.String()}}

	for _, tt := range []struct {
		name string
		fn   func() (*command.Command, error)
	}{
		{
			name: "NewCommand",
			fn: func() (*command.Command, error) {
				return git.NewExecCommandFactory(cfg).New(ctx, repo, subCmd, opt)
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := tt.fn()
			require.NoError(t, err)
			// There is no full setup, so executing the hook will fail.
			require.Error(t, cmd.Wait())

			var actualEnvVars []string
			for _, env := range cmd.Env() {
				kv := strings.SplitN(env, "=", 2)
				require.Len(t, kv, 2)
				key, val := kv[0], kv[1]

				if strings.HasPrefix(key, "GL_") || strings.HasPrefix(key, "GITALY_") {
					require.NotEmptyf(t, strings.TrimSpace(val),
						"env var %s value should not be empty string", key)
					actualEnvVars = append(actualEnvVars, key)
				}
			}

			require.EqualValues(t, []string{
				"GITALY_HOOKS_PAYLOAD",
				"GITALY_BIN_DIR",
				"GITALY_LOG_DIR",
			}, actualEnvVars)
		})
	}
}
