package hook

import (
	"context"
	"fmt"
	"io"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (m *GitLabHookManager) UpdateHook(ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
	payload, err := git.HooksPayloadFromEnv(env)
	if err != nil {
		return helper.ErrInternalf("extracting hooks payload: %w", err)
	}

	if isPrimary(payload) {
		if err := m.updateHook(ctx, payload, repo, ref, oldValue, newValue, env, stdout, stderr); err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Warn("stopping transaction because update hook failed")

			// If the update hook declines the push, then we need
			// to stop any secondaries voting on the transaction.
			if err := m.stopTransaction(ctx, payload); err != nil {
				ctxlogrus.Extract(ctx).WithError(err).Error("failed stopping transaction in update hook")
			}

			return err
		}
	}

	return nil
}

func (m *GitLabHookManager) updateHook(ctx context.Context, payload git.HooksPayload, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
	if ref == "" {
		return helper.ErrInternalf("hook got no reference")
	}
	if err := git.ValidateObjectID(oldValue); err != nil {
		return helper.ErrInternalf("hook got invalid old value: %w", err)
	}
	if err := git.ValidateObjectID(newValue); err != nil {
		return helper.ErrInternalf("hook got invalid new value: %w", err)
	}
	if payload.ReceiveHooksPayload == nil {
		return helper.ErrInternalf("payload has no receive hooks info")
	}

	executor, err := m.newCustomHooksExecutor(repo, "update")
	if err != nil {
		return helper.ErrInternal(err)
	}

	customEnv, err := m.customHooksEnv(payload, nil, env)
	if err != nil {
		return helper.ErrInternalf("constructing custom hook environment: %v", err)
	}

	if err = executor(
		ctx,
		[]string{ref, oldValue, newValue},
		customEnv,
		nil,
		stdout,
		stderr,
	); err != nil {
		return fmt.Errorf("executing custom hooks: %w", err)
	}

	return nil
}
