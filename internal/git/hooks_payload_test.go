package git

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestHooksPayload(t *testing.T) {
	repo := &gitalypb.Repository{
		StorageName:        "storage",
		RelativePath:       "relative/path",
		GitObjectDirectory: "object/directory",
		GitAlternateObjectDirectories: []string{
			"alternate/object/directory",
		},
		GlRepository:  "repository-1",
		GlProjectPath: "test/project",
	}

	tx := metadata.Transaction{
		ID:      1234,
		Node:    "primary",
		Primary: true,
	}

	praefect := metadata.PraefectServer{
		ListenAddr:    "127.0.0.1:1234",
		TLSListenAddr: "127.0.0.1:4321",
		SocketPath:    "/path/to/unix",
		Token:         "secret",
	}

	t.Run("envvar has proper name", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, nil, AllHooks).Env()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(env, EnvHooksPayload+"="))
	})

	t.Run("roundtrip succeeds", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, nil, PreReceiveHook).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{
			"UNRELATED=value",
			env,
			"ANOTHOR=unrelated-value",
			EnvHooksPayload + "_WITH_SUFFIX=is-ignored",
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:           repo,
			BinDir:         config.Config.BinDir,
			GitPath:        config.Config.Git.BinPath,
			InternalSocket: config.Config.GitalyInternalSocketPath(),
			RequestedHooks: PreReceiveHook,
		}, payload)
	})

	t.Run("roundtrip with transaction succeeds", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, &tx, &praefect, nil, UpdateHook).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{env})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:           repo,
			BinDir:         config.Config.BinDir,
			GitPath:        config.Config.Git.BinPath,
			InternalSocket: config.Config.GitalyInternalSocketPath(),
			Transaction:    &tx,
			Praefect:       &praefect,
			RequestedHooks: UpdateHook,
		}, payload)
	})

	t.Run("missing envvar", func(t *testing.T) {
		_, err := HooksPayloadFromEnv([]string{"OTHER_ENV=foobar"})
		require.Error(t, err)
		require.Equal(t, ErrPayloadNotFound, err)
	})

	t.Run("bogus value", func(t *testing.T) {
		_, err := HooksPayloadFromEnv([]string{EnvHooksPayload + "=foobar"})
		require.Error(t, err)
	})

	t.Run("payload with missing Praefect", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, &tx, nil, nil, AllHooks).Env()
		require.NoError(t, err)

		_, err = HooksPayloadFromEnv([]string{env})
		require.Equal(t, err, metadata.ErrPraefectServerNotFound)
	})

	t.Run("receive hooks payload", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, &ReceiveHooksPayload{
			UserID:   "1234",
			Username: "user",
			Protocol: "ssh",
		}, PostReceiveHook).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{
			env,
			"GL_ID=wrong",
			"GL_USERNAME=wrong",
			"GL_PROTOCOL=wrong",
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:                repo,
			BinDir:              config.Config.BinDir,
			GitPath:             config.Config.Git.BinPath,
			InternalSocket:      config.Config.GitalyInternalSocketPath(),
			InternalSocketToken: config.Config.Auth.Token,
			ReceiveHooksPayload: &ReceiveHooksPayload{
				UserID:   "1234",
				Username: "user",
				Protocol: "ssh",
			},
			RequestedHooks: PostReceiveHook,
		}, payload)
	})

	t.Run("payload with fallback git path", func(t *testing.T) {
		defer func(old string) {
			config.Config.Git.BinPath = old
		}(config.Config.Git.BinPath)
		config.Config.Git.BinPath = ""

		env, err := NewHooksPayload(config.Config, repo, nil, nil, nil, ReceivePackHooks).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{
			env,
			"GITALY_GIT_BIN_PATH=/foo/bar",
		})
		require.NoError(t, err)
		require.Equal(t, HooksPayload{
			Repo:           repo,
			BinDir:         config.Config.BinDir,
			GitPath:        "/foo/bar",
			InternalSocket: config.Config.GitalyInternalSocketPath(),
			RequestedHooks: ReceivePackHooks,
		}, payload)
	})
}

func TestHooksPayload_IsHookRequested(t *testing.T) {
	for _, tc := range []struct {
		desc       string
		configured Hook
		request    Hook
		expected   bool
	}{
		{
			desc:       "exact match",
			configured: PreReceiveHook,
			request:    PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "hook matches a set",
			configured: PreReceiveHook | PostReceiveHook,
			request:    PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "no match",
			configured: PreReceiveHook,
			request:    PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "no match with a set",
			configured: PreReceiveHook | UpdateHook,
			request:    PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "no match with nothing set",
			configured: 0,
			request:    PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "pre-receive hook with AllHooks",
			configured: AllHooks,
			request:    PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "post-receive hook with AllHooks",
			configured: AllHooks,
			request:    PostReceiveHook,
			expected:   true,
		},
		{
			desc:       "update hook with AllHooks",
			configured: AllHooks,
			request:    UpdateHook,
			expected:   true,
		},
		{
			desc:       "reference-transaction hook with AllHooks",
			configured: AllHooks,
			request:    ReferenceTransactionHook,
			expected:   true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual := HooksPayload{
				RequestedHooks: tc.configured,
			}.IsHookRequested(tc.request)
			require.Equal(t, tc.expected, actual)
		})
	}
}
