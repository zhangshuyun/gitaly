package git

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	// EnvHooksPayload is the name of the environment variable used
	// to hold the hooks payload.
	EnvHooksPayload = "GITALY_HOOKS_PAYLOAD"
)

var (
	jsonpbMarshaller   = &jsonpb.Marshaler{}
	jsonpbUnmarshaller = &jsonpb.Unmarshaler{}

	// ErrPayloadNotFound is returned by HooksPayloadFromEnv if the given
	// environment variables don't have a hooks payload.
	ErrPayloadNotFound = errors.New("no hooks payload found in environment")
)

// Hook represents a git hook. See githooks(5) for more information about
// existing hooks.
type Hook uint

const (
	// ReferenceTransactionHook represents the reference-transaction git hook.
	ReferenceTransactionHook = Hook(1 << iota)
	// UpdateHook represents the update git hook.
	UpdateHook
	// PreReceiveHook represents the pre-receive git hook.
	PreReceiveHook
	// PostReceiveHook represents the post-receive git hook.
	PostReceiveHook
	// PackObjectsHook represents the pack-objects git hook.
	PackObjectsHook

	// AllHooks is the bitwise set of all hooks supported by Gitaly.
	AllHooks = ReferenceTransactionHook | UpdateHook | PreReceiveHook | PostReceiveHook | PackObjectsHook
	// ReceivePackHooks includes the set of hooks which shall be executed in
	// a typical "push" or an emulation thereof (e.g. `updateReferenceWithHooks()`).
	ReceivePackHooks = ReferenceTransactionHook | UpdateHook | PreReceiveHook | PostReceiveHook
)

// HooksPayload holds parameters required for all hooks.
type HooksPayload struct {
	// RequestedHooks is a bitfield of requested Hooks. Hooks which
	// were not requested will not get executed.
	RequestedHooks Hook `json:"requested_hooks"`
	// FeatureFlags contains feature flags with their values. They are set
	// into the outgoing context when calling HookService.
	FeatureFlags featureflag.Raw `json:"feature_flags,omitempty"`

	// Repo is the repository in which the hook is running.
	Repo *gitalypb.Repository `json:"-"`
	// BinDir is the binary directory of Gitaly.
	BinDir string `json:"binary_directory"`
	// GitPath is the path to the git executable.
	GitPath string `json:"git_path"`
	// InternalSocket is the path to Gitaly's internal socket.
	InternalSocket string `json:"internal_socket"`
	// InternalSocketToken is the token required to authenticate with
	// Gitaly's internal socket.
	InternalSocketToken string `json:"internal_socket_token"`

	// Transaction is used to identify a reference transaction. This is an optional field -- if
	// it's not set, no transactional voting will happen.
	Transaction *txinfo.Transaction `json:"transaction"`

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
	UserID string `json:"userid"`
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
	tx *txinfo.Transaction,
	receiveHooksPayload *ReceiveHooksPayload,
	requestedHooks Hook,
	featureFlags featureflag.Raw,
) HooksPayload {
	return HooksPayload{
		Repo:                repo,
		BinDir:              cfg.BinDir,
		GitPath:             cfg.Git.BinPath,
		InternalSocket:      cfg.GitalyInternalSocketPath(),
		InternalSocketToken: cfg.Auth.Token,
		Transaction:         tx,
		ReceiveHooksPayload: receiveHooksPayload,
		RequestedHooks:      requestedHooks,
		FeatureFlags:        featureFlags,
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
	encoded, ok := lookupEnv(envs, EnvHooksPayload)
	if !ok {
		return HooksPayload{}, ErrPayloadNotFound
	}

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

	payload := jsonPayload.HooksPayload
	payload.Repo = &repo

	// If no git path is set up as part of the serialized hooks payload,
	// then we need to fall back to the old GITALY_GIT_BIN_PATH variable.
	// Ideally, we'd raise an error if the git path wasn't set via either
	// old or new way, but we can't as it wasn't previously injected in all
	// locations. So if we raised an error, then we'd now potentially cause
	// errors during migration.
	//
	// TODO: Remove this fallback code after a release was done which
	// includes this.
	if payload.GitPath == "" {
		payload.GitPath, _ = lookupEnv(envs, "GITALY_GIT_BIN_PATH")
	}

	// If no RequestedHooks are passed down to us, then we need to assume
	// that the caller of this hook isn't aware of this field and thus just
	// pretend that he wants to execute all hooks.
	if payload.RequestedHooks == 0 {
		payload.RequestedHooks = AllHooks
	}

	return payload, nil
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

	return fmt.Sprintf("%s=%s", EnvHooksPayload, encoded), nil
}

// IsHookRequested returns whether the HooksPayload is requesting execution of
// the given git hook.
func (p HooksPayload) IsHookRequested(hook Hook) bool {
	return p.RequestedHooks&hook != 0
}
