package rubyserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestStopSafe(t *testing.T) {
	badServers := []*Server{
		nil,
		New(config.Cfg{}),
	}

	for _, bs := range badServers {
		bs.Stop()
	}
}

func TestSetHeaders(t *testing.T) {
	cfg, repo, _, cleanup := testcfg.BuildWithRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	locator := config.NewLocator(cfg)

	testCases := []struct {
		desc    string
		repo    *gitalypb.Repository
		errType codes.Code
		setter  func(context.Context, storage.Locator, *gitalypb.Repository) (context.Context, error)
	}{
		{
			desc:    "SetHeaders invalid storage",
			repo:    &gitalypb.Repository{StorageName: "foo", RelativePath: "bar.git"},
			errType: codes.InvalidArgument,
			setter:  SetHeaders,
		},
		{
			desc:    "SetHeaders invalid rel path",
			repo:    &gitalypb.Repository{StorageName: repo.StorageName, RelativePath: "bar.git"},
			errType: codes.NotFound,
			setter:  SetHeaders,
		},
		{
			desc:    "SetHeaders OK",
			repo:    repo,
			errType: codes.OK,
			setter:  SetHeaders,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			clientCtx, err := tc.setter(ctx, locator, tc.repo)

			if tc.errType != codes.OK {
				testhelper.RequireGrpcError(t, err, tc.errType)
				assert.Nil(t, clientCtx)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, clientCtx)
			}
		})
	}
}

func TestSetupEnv(t *testing.T) {
	cfg := config.Cfg{
		BinDir:            "/bin/dit",
		InternalSocketDir: "/gitaly",
		Logging: config.Logging{
			Config: log.Config{
				Dir: "/log/dir",
			},
			RubySentryDSN: "testDSN",
			Sentry: config.Sentry{
				Environment: "testEnvironment",
			},
		},
		Git:  config.Git{BinPath: "/bin/git"},
		Auth: auth.Config{Token: "paswd"},
		Ruby: config.Ruby{RuggedGitConfigSearchPath: "/bin/rugged"},
	}

	env := setupEnv(cfg)

	require.Contains(t, env, "GITALY_LOG_DIR=/log/dir")
	require.Contains(t, env, "GITALY_RUBY_GIT_BIN_PATH=/bin/git")
	require.Contains(t, env, fmt.Sprintf("GITALY_RUBY_WRITE_BUFFER_SIZE=%d", streamio.WriteBufferSize))
	require.Contains(t, env, fmt.Sprintf("GITALY_RUBY_MAX_COMMIT_OR_TAG_MESSAGE_SIZE=%d", helper.MaxCommitOrTagMessageSize))
	require.Contains(t, env, "GITALY_RUBY_GITALY_BIN_DIR=/bin/dit")
	require.Contains(t, env, "GITALY_VERSION="+version.GetVersion())
	require.Contains(t, env, "GITALY_SOCKET=/gitaly/internal.sock")
	require.Contains(t, env, "GITALY_TOKEN=paswd")
	require.Contains(t, env, "GITALY_RUGGED_GIT_CONFIG_SEARCH_PATH=/bin/rugged")
	require.Contains(t, env, "SENTRY_DSN=testDSN")
	require.Contains(t, env, "SENTRY_ENVIRONMENT=testEnvironment")
}
