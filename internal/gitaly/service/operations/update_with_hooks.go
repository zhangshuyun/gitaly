package operations

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type preReceiveError struct {
	message string
}

func (e preReceiveError) Error() string {
	return e.message
}

type updateRefError struct {
	reference string
}

func (e updateRefError) Error() string {
	return fmt.Sprintf("Could not update %s. Please refresh and try again.", e.reference)
}

func hookErrorMessage(sout string, serr string, err error) string {
	if err != nil && errors.As(err, &hook.NotAllowedError{}) {
		return err.Error()
	}

	if len(strings.TrimSpace(serr)) > 0 {
		return serr
	}

	return sout
}

func (s *Server) updateReferenceWithHooks(
	ctx context.Context,
	repo *gitalypb.Repository,
	user *gitalypb.User,
	reference git.ReferenceName,
	newrev, oldrev git.ObjectID,
	pushOptions ...string,
) error {
	transaction, praefect, err := txinfo.FromContext(ctx)
	if err != nil {
		return err
	}

	payload, err := git.NewHooksPayload(s.cfg, repo, transaction, praefect, &git.ReceiveHooksPayload{
		UserID:   user.GetGlId(),
		Username: user.GetGlUsername(),
		Protocol: "web",
	}, git.ReceivePackHooks, featureflag.RawFromContext(ctx)).Env()
	if err != nil {
		return err
	}

	if reference == "" {
		return helper.ErrInternalf("updateReferenceWithHooks: got no reference")
	}
	if err := git.ValidateObjectID(oldrev.String()); err != nil {
		return helper.ErrInternalf("updateReferenceWithHooks: got invalid old value: %w", err)
	}
	if err := git.ValidateObjectID(newrev.String()); err != nil {
		return helper.ErrInternalf("updateReferenceWithHooks: got invalid new value: %w", err)
	}

	env := []string{
		payload,
	}

	changes := fmt.Sprintf("%s %s %s\n", oldrev, newrev, reference)
	var stdout, stderr bytes.Buffer

	if err := s.hookManager.PreReceiveHook(ctx, repo, pushOptions, env, strings.NewReader(changes), &stdout, &stderr); err != nil {
		msg := hookErrorMessage(stdout.String(), stderr.String(), err)
		return preReceiveError{message: msg}
	}
	if err := s.hookManager.UpdateHook(ctx, repo, reference.String(), oldrev.String(), newrev.String(), env, &stdout, &stderr); err != nil {
		msg := hookErrorMessage(stdout.String(), stderr.String(), err)
		return preReceiveError{message: msg}
	}

	if err := s.hookManager.ReferenceTransactionHook(ctx, hook.ReferenceTransactionPrepared, env, strings.NewReader(changes)); err != nil {
		return preReceiveError{message: err.Error()}
	}

	// We are already manually invoking the reference-transaction hook, so there is no need to
	// set up hooks again here. One could argue that it would be easier to just have git handle
	// execution of the reference-transaction hook. But unfortunately, it has proven to be
	// problematic: if we queue a deletion, and the reference to be deleted exists both as
	// packed-ref and as loose ref, then we would see two transactions: first a transaction
	// deleting the packed-ref which would otherwise get unshadowed by deleting the loose ref,
	// and only then do we see the deletion of the loose ref. So this depends on how well a repo
	// is packed, which is obviously a bad thing as Gitaly nodes may be differently packed. We
	// thus continue to manually drive the reference-transaction hook here, which doesn't have
	// this problem.
	updater, err := updateref.New(ctx, s.cfg, s.gitCmdFactory, repo, updateref.WithDisabledTransactions())
	if err != nil {
		return err
	}

	if err := updater.Update(reference, newrev.String(), oldrev.String()); err != nil {
		return err
	}

	if err := updater.Wait(); err != nil {
		return updateRefError{reference: reference.String()}
	}

	if err := s.hookManager.PostReceiveHook(ctx, repo, pushOptions, env, strings.NewReader(changes), &stdout, &stderr); err != nil {
		msg := hookErrorMessage(stdout.String(), stderr.String(), err)
		return preReceiveError{message: msg}
	}

	return nil
}
