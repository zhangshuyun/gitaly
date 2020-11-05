package git

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/protobuf/jsonpb"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitlabshell"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

var jsonpbMarshaller = &jsonpb.Marshaler{}

// WithRefTxHook returns an option that populates the safe command with the
// environment variables necessary to properly execute a reference hook for
// repository changes that may possibly update references
func WithRefTxHook(ctx context.Context, repo *gitalypb.Repository, cfg config.Cfg) CmdOpt {
	return func(cc *cmdCfg) error {
		if repo == nil {
			return fmt.Errorf("missing repo: %w", ErrInvalidArg)
		}

		rfEnvs, err := refHookEnv(ctx, repo, cfg)
		if err != nil {
			return fmt.Errorf("ref hook env var: %w", err)
		}

		cc.env = append(cc.env, rfEnvs...)
		cc.refHookConfigured = true

		return nil
	}
}

// refHookEnv returns all env vars required by the reference transaction hook
func refHookEnv(ctx context.Context, repo *gitalypb.Repository, cfg config.Cfg) ([]string, error) {
	repoJSON, err := jsonpbMarshaller.MarshalToString(repo)
	if err != nil {
		return nil, err
	}

	return []string{
		"GITALY_SOCKET=" + config.GitalyInternalSocketPath(),
		fmt.Sprintf("GITALY_REPO=%s", repoJSON),
		fmt.Sprintf("GITALY_TOKEN=%s", cfg.Auth.Token),
		fmt.Sprintf("%s=true", featureflag.ReferenceTransactionHookEnvVar),
	}, nil
}

// ReceivePackRequest abstracts away the different requests that end up
// spawning git-receive-pack.
type ReceivePackRequest interface {
	GetGlId() string
	GetGlUsername() string
	GetGlRepository() string
	GetRepository() *gitalypb.Repository
}

// WithReceivePackHooks returns an option that populates the safe command with the environment
// variables necessary to properly execute the pre-receive, update and post-receive hooks for
// git-receive-pack(1).
func WithReceivePackHooks(ctx context.Context, req ReceivePackRequest, protocol string) CmdOpt {
	return func(cc *cmdCfg) error {
		env, err := receivePackHookEnv(ctx, req, protocol)
		if err != nil {
			return fmt.Errorf("receive-pack hook envvars: %w", err)
		}

		cc.env = append(cc.env, env...)
		return nil
	}
}

func receivePackHookEnv(ctx context.Context, req ReceivePackRequest, protocol string) ([]string, error) {
	gitlabshellEnv, err := gitlabshell.Env()
	if err != nil {
		return nil, err
	}

	env, err := refHookEnv(ctx, req.GetRepository(), config.Config)
	if err != nil {
		return nil, err
	}

	env = append(env,
		fmt.Sprintf("GL_ID=%s", req.GetGlId()),
		fmt.Sprintf("GL_USERNAME=%s", req.GetGlUsername()),
		fmt.Sprintf("GL_REPOSITORY=%s", req.GetGlRepository()),
		fmt.Sprintf("GL_PROJECT_PATH=%s", req.GetRepository().GetGlProjectPath()),
		fmt.Sprintf("GL_PROTOCOL=%s", protocol),
		fmt.Sprintf("%s=true", featureflag.ReferenceTransactionHookEnvVar),
	)
	env = append(env, gitlabshellEnv...)

	transaction, err := metadata.TransactionFromContext(ctx)
	if err == nil {
		praefect, err := metadata.PraefectFromContext(ctx)
		if err != nil {
			return nil, err
		}

		praefectEnv, err := praefect.Env()
		if err != nil {
			return nil, err
		}

		transactionEnv, err := transaction.Env()
		if err != nil {
			return nil, err
		}

		env = append(env, praefectEnv, transactionEnv)
	} else if !errors.Is(err, metadata.ErrTransactionNotFound) {
		return nil, err
	}

	return env, nil
}
