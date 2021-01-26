package rubyserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestStopSafe(t *testing.T) {
	badServers := []*Server{
		nil,
		&Server{},
	}

	for _, bs := range badServers {
		bs.Stop()
	}
}

func TestSetHeaders(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	locator := testhelper.DefaultLocator()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

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
			repo:    &gitalypb.Repository{StorageName: testRepo.StorageName, RelativePath: "bar.git"},
			errType: codes.NotFound,
			setter:  SetHeaders,
		},
		{
			desc:    "SetHeaders OK",
			repo:    testRepo,
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
	cfg := config.Config
	cfg.Logging.RubySentryDSN = "testDSN"
	cfg.Logging.Sentry.Environment = "testEnvironment"
	env := setupEnv(cfg)

	require.Contains(t, env, "GITALY_LOG_DIR="+cfg.Logging.Dir)
	require.Contains(t, env, "GITALY_RUBY_GIT_BIN_PATH="+cfg.Git.BinPath)
	require.Contains(t, env, fmt.Sprintf("GITALY_RUBY_WRITE_BUFFER_SIZE=%d", streamio.WriteBufferSize))
	require.Contains(t, env, fmt.Sprintf("GITALY_RUBY_MAX_COMMIT_OR_TAG_MESSAGE_SIZE=%d", helper.MaxCommitOrTagMessageSize))
	require.Contains(t, env, "GITALY_RUBY_GITALY_BIN_DIR="+cfg.BinDir)
	require.Contains(t, env, "GITALY_VERSION="+version.GetVersion())
	require.Contains(t, env, "GITALY_SOCKET="+cfg.GitalyInternalSocketPath())
	require.Contains(t, env, "GITALY_TOKEN="+cfg.Auth.Token)
	require.Contains(t, env, "GITALY_RUGGED_GIT_CONFIG_SEARCH_PATH="+cfg.Ruby.RuggedGitConfigSearchPath)
	require.Contains(t, env, "SENTRY_DSN=testDSN")
	require.Contains(t, env, "SENTRY_ENVIRONMENT=testEnvironment")
}
