package git

import (
	"strings"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestHooksPayload(t *testing.T) {
	repo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	marshaller := &jsonpb.Marshaler{}
	marshalledRepo, err := marshaller.MarshalToString(repo)
	require.NoError(t, err)

	t.Run("envvar has proper name", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, nil).Env()
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(env, ErrHooksPayloadNotFound+"="))
	})

	tx := metadata.Transaction{
		ID:      1234,
		Node:    "primary",
		Primary: true,
	}
	txEnv, err := tx.Env()
	require.NoError(t, err)

	praefect := metadata.PraefectServer{
		ListenAddr:    "127.0.0.1:1234",
		TLSListenAddr: "127.0.0.1:4321",
		SocketPath:    "/path/to/unix",
		Token:         "secret",
	}
	praefectEnv, err := praefect.Env()
	require.NoError(t, err)

	t.Run("roundtrip succeeds", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, nil).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{
			"UNRELATED=value",
			env,
			"ANOTHOR=unrelated-value",
			ErrHooksPayloadNotFound + "_WITH_SUFFIX=is-ignored",
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:           repo,
			BinDir:         config.Config.BinDir,
			InternalSocket: config.Config.GitalyInternalSocketPath(),
		}, payload)
	})

	t.Run("roundtrip with transaction succeeds", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, &tx, &praefect, nil).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{env})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:           repo,
			BinDir:         config.Config.BinDir,
			InternalSocket: config.Config.GitalyInternalSocketPath(),
			Transaction:    &tx,
			Praefect:       &praefect,
		}, payload)
	})

	t.Run("missing envvar", func(t *testing.T) {
		_, err := HooksPayloadFromEnv([]string{"OTHER_ENV=foobar"})
		require.Error(t, err)
		require.Equal(t, ErrPayloadNotFound, err)
	})

	t.Run("bogus value", func(t *testing.T) {
		_, err := HooksPayloadFromEnv([]string{ErrHooksPayloadNotFound + "=foobar"})
		require.Error(t, err)
	})

	t.Run("fallback", func(t *testing.T) {
		payload, err := HooksPayloadFromEnv([]string{
			"GITALY_BIN_DIR=/bin/dir",
			"GITALY_SOCKET=/path/to/socket",
			"GITALY_TOKEN=secret",
			"GITALY_REPOSITORY=" + marshalledRepo,
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:                repo,
			BinDir:              "/bin/dir",
			InternalSocket:      "/path/to/socket",
			InternalSocketToken: "secret",
		}, payload)
	})

	t.Run("fallback without token", func(t *testing.T) {
		payload, err := HooksPayloadFromEnv([]string{
			"GITALY_BIN_DIR=/bin/dir",
			"GITALY_SOCKET=/path/to/socket",
			"GITALY_REPOSITORY=" + marshalledRepo,
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:           repo,
			BinDir:         "/bin/dir",
			InternalSocket: "/path/to/socket",
		}, payload)
	})

	t.Run("fallback with missing repository", func(t *testing.T) {
		_, err := HooksPayloadFromEnv([]string{
			"GITALY_BIN_DIR=/bin/dir",
			"GITALY_SOCKET=/path/to/socket",
		})
		require.Equal(t, ErrPayloadNotFound, err)
	})

	t.Run("fallback with missing bindir", func(t *testing.T) {
		_, err := HooksPayloadFromEnv([]string{
			"GITALY_SOCKET=/path/to/socket",
			"GITALY_REPOSITORY=" + marshalledRepo,
		})
		require.Equal(t, ErrPayloadNotFound, err)
	})

	t.Run("fallback with missing socket", func(t *testing.T) {
		_, err := HooksPayloadFromEnv([]string{
			"GITALY_BIN_DIR=/bin/dir",
			"GITALY_REPOSITORY=" + marshalledRepo,
		})
		require.Equal(t, ErrPayloadNotFound, err)
	})

	t.Run("fallback with transaction", func(t *testing.T) {
		payload, err := HooksPayloadFromEnv([]string{
			"GITALY_BIN_DIR=/bin/dir",
			"GITALY_SOCKET=/path/to/socket",
			"GITALY_TOKEN=secret",
			"GITALY_REPOSITORY=" + marshalledRepo,
			txEnv,
			praefectEnv,
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:                repo,
			BinDir:              "/bin/dir",
			InternalSocket:      "/path/to/socket",
			InternalSocketToken: "secret",
			Transaction:         &tx,
			Praefect:            &praefect,
		}, payload)
	})

	t.Run("fallback with only Praefect env", func(t *testing.T) {
		payload, err := HooksPayloadFromEnv([]string{
			"GITALY_BIN_DIR=/bin/dir",
			"GITALY_SOCKET=/path/to/socket",
			"GITALY_TOKEN=secret",
			"GITALY_REPOSITORY=" + marshalledRepo,
			praefectEnv,
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:                repo,
			BinDir:              "/bin/dir",
			InternalSocket:      "/path/to/socket",
			InternalSocketToken: "secret",
		}, payload)
	})

	t.Run("fallback with missing Praefect", func(t *testing.T) {
		_, err := HooksPayloadFromEnv([]string{
			"GITALY_BIN_DIR=/bin/dir",
			"GITALY_SOCKET=/path/to/socket",
			"GITALY_TOKEN=secret",
			"GITALY_REPOSITORY=" + marshalledRepo,
			txEnv,
		})
		require.Equal(t, err, metadata.ErrPraefectServerNotFound)
	})

	t.Run("payload with missing Praefect", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, nil).Env()
		require.NoError(t, err)

		_, err = HooksPayloadFromEnv([]string{env, txEnv})
		require.Equal(t, err, metadata.ErrPraefectServerNotFound)
	})

	t.Run("payload with fallback transaction", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, nil).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{env, txEnv, praefectEnv})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:                repo,
			BinDir:              config.Config.BinDir,
			InternalSocket:      config.Config.GitalyInternalSocketPath(),
			InternalSocketToken: config.Config.Auth.Token,
			Transaction:         &tx,
			Praefect:            &praefect,
		}, payload)
	})

	t.Run("fallback with GL_ values", func(t *testing.T) {
		payload, err := HooksPayloadFromEnv([]string{
			"GITALY_BIN_DIR=/bin/dir",
			"GITALY_SOCKET=/path/to/socket",
			"GITALY_TOKEN=secret",
			"GITALY_REPOSITORY=" + marshalledRepo,
			"GL_ID=1234",
			"GL_USERNAME=user",
			"GL_PROTOCOL=ssh",
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:                repo,
			BinDir:              "/bin/dir",
			InternalSocket:      "/path/to/socket",
			InternalSocketToken: "secret",
			ReceiveHooksPayload: &ReceiveHooksPayload{
				UserID:   1234,
				Username: "user",
				Protocol: "ssh",
			},
		}, payload)
	})

	t.Run("payload with GL_ values", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, nil).Env()
		require.NoError(t, err)

		payload, err := HooksPayloadFromEnv([]string{
			env,
			"GL_ID=1234",
			"GL_USERNAME=user",
			"GL_PROTOCOL=ssh",
		})
		require.NoError(t, err)

		require.Equal(t, HooksPayload{
			Repo:                repo,
			BinDir:              config.Config.BinDir,
			InternalSocket:      config.Config.GitalyInternalSocketPath(),
			InternalSocketToken: config.Config.Auth.Token,
			ReceiveHooksPayload: &ReceiveHooksPayload{
				UserID:   1234,
				Username: "user",
				Protocol: "ssh",
			},
		}, payload)
	})

	t.Run("payload with missing GL_ID", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, nil).Env()
		require.NoError(t, err)

		_, err = HooksPayloadFromEnv([]string{
			env,
			"GL_USERNAME=user",
			"GL_PROTOCOL=ssh",
		})
		require.Error(t, err)
		require.Equal(t, "no user ID found in hooks environment", err.Error())
	})

	t.Run("payload with non-numeric GL_ID", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, nil).Env()
		require.NoError(t, err)

		_, err = HooksPayloadFromEnv([]string{
			env,
			"GL_USERNAME=user",
			"GL_ID=1234xyz",
			"GL_PROTOCOL=ssh",
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "user ID found in hooks is non-numeric")
	})

	t.Run("receive hooks payload", func(t *testing.T) {
		env, err := NewHooksPayload(config.Config, repo, nil, nil, &ReceiveHooksPayload{
			UserID:   1234,
			Username: "user",
			Protocol: "ssh",
		}).Env()
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
			InternalSocket:      config.Config.GitalyInternalSocketPath(),
			InternalSocketToken: config.Config.Auth.Token,
			ReceiveHooksPayload: &ReceiveHooksPayload{
				UserID:   1234,
				Username: "user",
				Protocol: "ssh",
			},
		}, payload)
	})
}
