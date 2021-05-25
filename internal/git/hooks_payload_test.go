package git_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
)

func TestHooksPayload(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	tx := txinfo.Transaction{
		ID:      1234,
		Node:    "primary",
		Primary: true,
	}

	praefect := txinfo.PraefectServer{
		BackchannelID: 1,
		ListenAddr:    "127.0.0.1:1234",
		TLSListenAddr: "127.0.0.1:4321",
		SocketPath:    "/path/to/unix",
		Token:         "secret",
	}

	t.Run("envvar has proper name", func(t *testing.T) {
		env, err := git.NewHooksPayload(cfg, repo, nil, nil, nil, git.AllHooks, nil).Env()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(env, git.EnvHooksPayload+"="))
	})

	t.Run("roundtrip succeeds", func(t *testing.T) {
		env, err := git.NewHooksPayload(cfg, repo, nil, nil, nil, git.PreReceiveHook, featureflag.Raw{"flag-key": "flag-value"}).Env()
		require.NoError(t, err)

		payload, err := git.HooksPayloadFromEnv([]string{
			"UNRELATED=value",
			env,
			"ANOTHOR=unrelated-value",
			git.EnvHooksPayload + "_WITH_SUFFIX=is-ignored",
		})
		require.NoError(t, err)

		require.Equal(t, git.HooksPayload{
			Repo:           repo,
			BinDir:         cfg.BinDir,
			GitPath:        cfg.Git.BinPath,
			InternalSocket: cfg.GitalyInternalSocketPath(),
			RequestedHooks: git.PreReceiveHook,
			FeatureFlags:   featureflag.Raw{"flag-key": "flag-value"},
		}, payload)
	})

	t.Run("roundtrip with transaction succeeds", func(t *testing.T) {
		env, err := git.NewHooksPayload(cfg, repo, &tx, &praefect, nil, git.UpdateHook, nil).Env()
		require.NoError(t, err)

		payload, err := git.HooksPayloadFromEnv([]string{env})
		require.NoError(t, err)

		require.Equal(t, git.HooksPayload{
			Repo:           repo,
			BinDir:         cfg.BinDir,
			GitPath:        cfg.Git.BinPath,
			InternalSocket: cfg.GitalyInternalSocketPath(),
			Transaction:    &tx,
			Praefect:       &praefect,
			RequestedHooks: git.UpdateHook,
		}, payload)
	})

	t.Run("missing envvar", func(t *testing.T) {
		_, err := git.HooksPayloadFromEnv([]string{"OTHER_ENV=foobar"})
		require.Error(t, err)
		require.Equal(t, git.ErrPayloadNotFound, err)
	})

	t.Run("bogus value", func(t *testing.T) {
		_, err := git.HooksPayloadFromEnv([]string{git.EnvHooksPayload + "=foobar"})
		require.Error(t, err)
	})

	t.Run("payload with missing Praefect", func(t *testing.T) {
		env, err := git.NewHooksPayload(cfg, repo, &tx, nil, nil, git.AllHooks, nil).Env()
		require.NoError(t, err)

		_, err = git.HooksPayloadFromEnv([]string{env})
		require.Equal(t, err, txinfo.ErrPraefectServerNotFound)
	})

	t.Run("receive hooks payload", func(t *testing.T) {
		env, err := git.NewHooksPayload(cfg, repo, nil, nil, &git.ReceiveHooksPayload{
			UserID:   "1234",
			Username: "user",
			Protocol: "ssh",
		}, git.PostReceiveHook, nil).Env()
		require.NoError(t, err)

		payload, err := git.HooksPayloadFromEnv([]string{
			env,
			"GL_ID=wrong",
			"GL_USERNAME=wrong",
			"GL_PROTOCOL=wrong",
		})
		require.NoError(t, err)

		require.Equal(t, git.HooksPayload{
			Repo:                repo,
			BinDir:              cfg.BinDir,
			GitPath:             cfg.Git.BinPath,
			InternalSocket:      cfg.GitalyInternalSocketPath(),
			InternalSocketToken: cfg.Auth.Token,
			ReceiveHooksPayload: &git.ReceiveHooksPayload{
				UserID:   "1234",
				Username: "user",
				Protocol: "ssh",
			},
			RequestedHooks: git.PostReceiveHook,
		}, payload)
	})

	t.Run("payload with fallback git path", func(t *testing.T) {
		cfg, repo, _ := testcfg.BuildWithRepo(t)
		cfg.Git.BinPath = ""

		env, err := git.NewHooksPayload(cfg, repo, nil, nil, nil, git.ReceivePackHooks, nil).Env()
		require.NoError(t, err)

		payload, err := git.HooksPayloadFromEnv([]string{
			env,
			"GITALY_GIT_BIN_PATH=/foo/bar",
		})
		require.NoError(t, err)
		require.Equal(t, git.HooksPayload{
			Repo:           repo,
			BinDir:         cfg.BinDir,
			GitPath:        "/foo/bar",
			InternalSocket: cfg.GitalyInternalSocketPath(),
			RequestedHooks: git.ReceivePackHooks,
		}, payload)
	})
}

func TestHooksPayload_IsHookRequested(t *testing.T) {
	for _, tc := range []struct {
		desc       string
		configured git.Hook
		request    git.Hook
		expected   bool
	}{
		{
			desc:       "exact match",
			configured: git.PreReceiveHook,
			request:    git.PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "hook matches a set",
			configured: git.PreReceiveHook | git.PostReceiveHook,
			request:    git.PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "no match",
			configured: git.PreReceiveHook,
			request:    git.PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "no match with a set",
			configured: git.PreReceiveHook | git.UpdateHook,
			request:    git.PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "no match with nothing set",
			configured: 0,
			request:    git.PostReceiveHook,
			expected:   false,
		},
		{
			desc:       "pre-receive hook with AllHooks",
			configured: git.AllHooks,
			request:    git.PreReceiveHook,
			expected:   true,
		},
		{
			desc:       "post-receive hook with AllHooks",
			configured: git.AllHooks,
			request:    git.PostReceiveHook,
			expected:   true,
		},
		{
			desc:       "update hook with AllHooks",
			configured: git.AllHooks,
			request:    git.UpdateHook,
			expected:   true,
		},
		{
			desc:       "reference-transaction hook with AllHooks",
			configured: git.AllHooks,
			request:    git.ReferenceTransactionHook,
			expected:   true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual := git.HooksPayload{
				RequestedHooks: tc.configured,
			}.IsHookRequested(tc.request)
			require.Equal(t, tc.expected, actual)
		})
	}
}
