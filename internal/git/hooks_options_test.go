package git

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestWithRefHook(t *testing.T) {
	storageRoot, cleanup := testhelper.TempDir(t)
	defer cleanup()

	cfg := config.Config
	cfg.Auth.Token = "my-super-secure-token"
	cfg.Storages = []config.Storage{
		{
			Name: "storage",
			Path: storageRoot,
		},
	}

	repoPath := filepath.Join(storageRoot, "repo.git")
	testhelper.MustRunCommand(t, nil, "git", "init", "--bare", repoPath)

	repo := &gitalypb.Repository{
		StorageName:   "storage",
		RelativePath:  "repo.git",
		GlRepository:  "repository-1",
		GlProjectPath: "test/project",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	opt := WithRefTxHook(ctx, repo, cfg)
	subCmd := SubCmd{Name: "update-ref", Args: []string{"refs/heads/master", ZeroOID.String()}}

	for _, tt := range []struct {
		name string
		fn   func() (*command.Command, error)
	}{
		{
			name: "NewCommand",
			fn: func() (*command.Command, error) {
				return NewExecCommandFactory(cfg).New(ctx, repo, nil, subCmd, opt)
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
