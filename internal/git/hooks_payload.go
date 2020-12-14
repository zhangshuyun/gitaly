package git

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"strconv"

	"github.com/golang/protobuf/jsonpb"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	// ErrHooksPayloadNotFound is the name of the environment variable used
	// to hold the hooks payload.
	ErrHooksPayloadNotFound = "GITALY_HOOKS_PAYLOAD"
)

var (
	jsonpbMarshaller   = &jsonpb.Marshaler{}
	jsonpbUnmarshaller = &jsonpb.Unmarshaler{}

	// ErrPayloadNotFound is returned by HooksPayloadFromEnv if the given
	// environment variables don't have a hooks payload.
	ErrPayloadNotFound = errors.New("no hooks payload found in environment")
)

// HooksPayload holds parameters required for all hooks.
type HooksPayload struct {
	// Repo is the repository in which the hook is running.
	Repo *gitalypb.Repository `json:"-"`
	// BinDir is the binary directory of Gitaly.
	BinDir string `json:"binary_directory"`
	// InternalSocket is the path to Gitaly's internal socket.
	InternalSocket string `json:"internal_socket"`
	// InternalSocketToken is the token required to authenticate with
	// Gitaly's internal socket.
	InternalSocketToken string `json:"internal_socket_token"`

	// Transaction is used to identify a reference transaction. This is an optional field -- if
	// it's not set, no transactional voting will happen.
	Transaction *metadata.Transaction `json:"transaction"`
	// Praefect is used to identify the Praefect server which is hosting the transaction. This
	// field must be set if and only if `Transaction` is.
	Praefect *metadata.PraefectServer `json:"praefect"`

	// ReceiveHooksPayload contains information required when executing
	// git-receive-pack.
	ReceiveHooksPayload *ReceiveHooksPayload `json:"receive_hooks_payload"`
}

// ReceiveHooksPayload contains all information which is required for hooks
// executed by git-receive-pack, namely the pre-receive, update or post-receive
// hook.
type ReceiveHooksPayload struct {
	// Username contains the name of the user who has caused the hook to be executed.
	Username string `json:"username"`
	// UserID contains the ID of the user who has caused the hook to be executed.
	UserID int64 `json:"userid"`
	// Protocol contains the protocol via which the hook was executed. This
	// can be one of "web", "ssh" or "smarthttp".
	Protocol string `json:"protocol"`
}

// jsonHooksPayload wraps the HooksPayload such that we can manually encode the
// repository protobuf message.
type jsonHooksPayload struct {
	HooksPayload
	Repo string `json:"repository"`
}

// NewHooksPayload creates a new hooks payload which can then be encoded and
// passed to Git hooks.
func NewHooksPayload(
	cfg config.Cfg,
	repo *gitalypb.Repository,
	tx *metadata.Transaction,
	praefect *metadata.PraefectServer,
	receiveHooksPayload *ReceiveHooksPayload,
) HooksPayload {
	return HooksPayload{
		Repo:                repo,
		BinDir:              cfg.BinDir,
		InternalSocket:      cfg.GitalyInternalSocketPath(),
		InternalSocketToken: cfg.Auth.Token,
		Transaction:         tx,
		Praefect:            praefect,
		ReceiveHooksPayload: receiveHooksPayload,
	}
}

func lookupEnv(envs []string, key string) (string, bool) {
	for _, env := range envs {
		kv := strings.SplitN(env, "=", 2)
		if len(kv) != 2 {
			continue
		}

		if kv[0] == key {
			return kv[1], true
		}
	}

	return "", false
}

// HooksPayloadFromEnv extracts the HooksPayload from the given environment
// variables. If no HooksPayload exists, it returns a ErrPayloadNotFound
// error.
func HooksPayloadFromEnv(envs []string) (HooksPayload, error) {
	var payload HooksPayload

	if encoded, ok := lookupEnv(envs, ErrHooksPayloadNotFound); ok {
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return HooksPayload{}, err
		}

		var jsonPayload jsonHooksPayload
		if err := json.Unmarshal(decoded, &jsonPayload); err != nil {
			return HooksPayload{}, err
		}

		var repo gitalypb.Repository
		err = jsonpbUnmarshaller.Unmarshal(strings.NewReader(jsonPayload.Repo), &repo)
		if err != nil {
			return HooksPayload{}, err
		}

		payload = jsonPayload.HooksPayload
		payload.Repo = &repo
	} else {
		fallback, err := fallbackPayloadFromEnv(envs)
		if err != nil {
			return HooksPayload{}, err
		}

		payload = fallback
	}

	// If we didn't find a transaction, then we need to fall back to the
	// transaction environment variables with the same reasoning as for
	// `fallbackPayloadFromEnv()`.
	if payload.Transaction == nil {
		tx, err := metadata.TransactionFromEnv(envs)
		if err == nil {
			praefect, err := metadata.PraefectFromEnv(envs)
			if err != nil {
				return HooksPayload{}, err
			}

			payload.Transaction = &tx
			payload.Praefect = praefect
		} else if err != metadata.ErrTransactionNotFound {
			return HooksPayload{}, err
		}
	}

	// If we didn't find a ReceiveHooksPayload, then we need to fallback to
	// the GL_ environment values with the same reasoning as for
	// `fallbackPayloadFromEnv()`.
	if payload.ReceiveHooksPayload == nil {
		receiveHooksPayload, err := fallbackReceiveHooksPayloadFromEnv(envs)
		if err != nil {
			return HooksPayload{}, err
		}
		payload.ReceiveHooksPayload = receiveHooksPayload
	}

	return payload, nil
}

// fallbackPayloadFromEnv is a compatibility layer for upgrades where it may
// happen that the new gitaly-hooks binary has already been installed while the
// old server is still running. As a result, there'd be some time where we
// don't yet have GITALY_HOOKS_PAYLOAD set up in our environment, and we'll
// have to cope with this. We thus fall back to the old envvars here.
func fallbackPayloadFromEnv(envs []string) (HooksPayload, error) {
	var payload HooksPayload

	marshalledRepo, ok := lookupEnv(envs, "GITALY_REPOSITORY")
	if !ok {
		return payload, ErrPayloadNotFound
	}

	var repo gitalypb.Repository
	if err := jsonpbUnmarshaller.Unmarshal(strings.NewReader(marshalledRepo), &repo); err != nil {
		return HooksPayload{}, err
	}
	payload.Repo = &repo

	binDir, ok := lookupEnv(envs, "GITALY_BIN_DIR")
	if !ok {
		return payload, ErrPayloadNotFound
	}
	payload.BinDir = binDir

	socket, ok := lookupEnv(envs, "GITALY_SOCKET")
	if !ok {
		return payload, ErrPayloadNotFound
	}
	payload.InternalSocket = socket

	// The token may be unset, which is fine if no authentication is
	// required.
	token, _ := lookupEnv(envs, "GITALY_TOKEN")
	payload.InternalSocketToken = token

	return payload, nil
}

// fallbackReceiveHooksPayloadFromEnv is a compatibility layer for upgrades
// where it may happen that the new gitaly-hooks binary has already been
// installed while the old server is still running.
//
// We need to keep in mind that it's perfectly fine for hooks to not have
// the GL_ values in case we only run the reference-transaction hook. We cannot
// distinguish those cases, so the best we can do is check for the first value
// to exist: if it exists, we assume all the others must exist as well.
// Otherwise, we assume none exist. This should be fine as all three hooks
// expect those values to be set, while the reference-transaction hook doesn't
// care at all for them.
func fallbackReceiveHooksPayloadFromEnv(envs []string) (*ReceiveHooksPayload, error) {
	protocol, ok := lookupEnv(envs, "GL_PROTOCOL")
	if !ok {
		return nil, nil
	}

	userID, ok := lookupEnv(envs, "GL_ID")
	if !ok {
		return nil, errors.New("no user ID found in hooks environment")
	}
	userIDint, err := strconv.ParseInt(userID, 10, 0)
	if err != nil {
		return nil, fmt.Errorf("user ID found in hooks is non-numeric: %w", err)
	}

	username, ok := lookupEnv(envs, "GL_USERNAME")
	if !ok {
		return nil, errors.New("no user ID found in hooks environment")
	}

	return &ReceiveHooksPayload{
		Protocol: protocol,
		UserID:   userIDint,
		Username: username,
	}, nil
}

// Env encodes the given HooksPayload into an environment variable.
func (p HooksPayload) Env() (string, error) {
	repo, err := jsonpbMarshaller.MarshalToString(p.Repo)
	if err != nil {
		return "", err
	}

	jsonPayload := jsonHooksPayload{p, repo}
	marshalled, err := json.Marshal(jsonPayload)
	if err != nil {
		return "", err
	}

	encoded := base64.StdEncoding.EncodeToString(marshalled)

	return fmt.Sprintf("%s=%s", ErrHooksPayloadNotFound, encoded), nil
}
