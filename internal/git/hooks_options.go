package git

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// WithDisabledHooks returns an option that satisfies the requirement to set up
// hooks, but won't in fact set up hook execution.
func WithDisabledHooks() CmdOpt {
	return func(cc *cmdCfg) error {
		cc.hooksConfigured = true
		return nil
	}
}

// WithRefTxHook returns an option that populates the safe command with the
// environment variables necessary to properly execute a reference hook for
// repository changes that may possibly update references
func WithRefTxHook(ctx context.Context, repo *gitalypb.Repository, cfg config.Cfg) CmdOpt {
	return func(cc *cmdCfg) error {
		if repo == nil {
			return fmt.Errorf("missing repo: %w", ErrInvalidArg)
		}

		if err := cc.configureHooks(ctx, repo, cfg, nil); err != nil {
			return fmt.Errorf("ref hook env var: %w", err)
		}

		return nil
	}
}

// WithPackObjectsHookEnv provides metadata for gitaly-hooks so it can act as a pack-objects hook.
func WithPackObjectsHookEnv(ctx context.Context, repo *gitalypb.Repository, cfg config.Cfg) CmdOpt {
	return func(cc *cmdCfg) error {
		if repo == nil {
			return fmt.Errorf("missing repo: %w", ErrInvalidArg)
		}

		payload, err := NewHooksPayload(cfg, repo, nil, nil, nil).Env()
		if err != nil {
			return err
		}

		cc.env = append(
			cc.env,
			payload,
			"GITALY_BIN_DIR="+cfg.BinDir,
			"GITALY_GIT_BIN_PATH="+cfg.Git.BinPath,
			fmt.Sprintf("%s=%s", log.GitalyLogDirEnvKey, cfg.Logging.Dir),
		)

		return nil
	}
}

// configureHooks updates the command configuration to include all environment
// variables required by the reference transaction hook and any other needed
// options to successfully execute hooks.
func (cc *cmdCfg) configureHooks(
	ctx context.Context,
	repo *gitalypb.Repository,
	cfg config.Cfg,
	receiveHooksPayload *ReceiveHooksPayload,
) error {
	if cc.hooksConfigured {
		return errors.New("hooks already configured")
	}

	transaction, praefect, err := metadata.TransactionMetadataFromContext(ctx)
	if err != nil {
		return err
	}

	payload, err := NewHooksPayload(cfg, repo, transaction, praefect, receiveHooksPayload).Env()
	if err != nil {
		return err
	}

	cc.env = append(
		cc.env,
		payload,
		"GITALY_BIN_DIR="+cfg.BinDir,
		fmt.Sprintf("%s=%s", log.GitalyLogDirEnvKey, cfg.Logging.Dir),
	)

	cc.globals = append(cc.globals, ConfigPair{Key: "core.hooksPath", Value: hooks.Path(cfg)})
	cc.hooksConfigured = true

	return nil
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
func WithReceivePackHooks(ctx context.Context, cfg config.Cfg, req ReceivePackRequest, protocol string) CmdOpt {
	return func(cc *cmdCfg) error {
		if err := cc.configureHooks(ctx, req.GetRepository(), cfg, &ReceiveHooksPayload{
			UserID:   req.GetGlId(),
			Username: req.GetGlUsername(),
			Protocol: protocol,
		}); err != nil {
			return err
		}

		return nil
	}
}
