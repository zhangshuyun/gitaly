package updateref

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// UpdaterWithHooks updates a ref with Git hooks.
type UpdaterWithHooks struct {
	cfg           config.Cfg
	hookManager   hook.Manager
	gitCmdFactory git.CommandFactory
	catfileCache  catfile.Cache
}

// PreReceiveError contains an error message for a git pre-receive failure
type PreReceiveError struct {
	Message string
}

func (e PreReceiveError) Error() string {
	return e.Message
}

// Error reports an error in git update-ref
type Error struct {
	reference string
}

func (e Error) Error() string {
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

// NewUpdaterWithHooks creates a new instance of a struct that will update a Git reference.
func NewUpdaterWithHooks(
	cfg config.Cfg,
	hookManager hook.Manager,
	gitCmdFactory git.CommandFactory,
	catfileCache catfile.Cache,
) *UpdaterWithHooks {
	return &UpdaterWithHooks{
		cfg:           cfg,
		hookManager:   hookManager,
		gitCmdFactory: gitCmdFactory,
		catfileCache:  catfileCache,
	}
}

// UpdateReference updates a branch with a given commit ID using the Git hooks. If a quarantine
// directory is given, then the pre-receive, update and reference-transaction hook will be invoked
// with the quarantined repository as returned by the quarantine structure. If these hooks succeed,
// quarantined objects will be migrated and all subsequent hooks are executed via the unquarantined
// repository.
func (u *UpdaterWithHooks) UpdateReference(
	ctx context.Context,
	repo *gitalypb.Repository,
	user *gitalypb.User,
	quarantineDir *quarantine.Dir,
	reference git.ReferenceName,
	newrev, oldrev git.ObjectID,
	pushOptions ...string,
) error {
	var transaction *txinfo.Transaction
	if tx, err := txinfo.TransactionFromContext(ctx); err == nil {
		transaction = &tx
	} else if !errors.Is(err, txinfo.ErrTransactionNotFound) {
		return err
	}

	if reference == "" {
		return helper.ErrInternalf("UpdateReference: got no reference")
	}
	if err := git.ValidateObjectID(oldrev.String()); err != nil {
		return helper.ErrInternalf("UpdateReference: got invalid old value: %w", err)
	}
	if err := git.ValidateObjectID(newrev.String()); err != nil {
		return helper.ErrInternalf("UpdateReference: got invalid new value: %w", err)
	}

	changes := fmt.Sprintf("%s %s %s\n", oldrev, newrev, reference)

	receiveHooksPayload := git.ReceiveHooksPayload{
		UserID:   user.GetGlId(),
		Username: user.GetGlUsername(),
		Protocol: "web",
	}

	// In case there's no quarantine directory, we simply take the normal unquarantined
	// repository as input for the hooks payload. Otherwise, we'll take the quarantined
	// repository, which carries information about the quarantined object directory. This is
	// then subsequently passed to Rails, which can use the quarantine directory to more
	// efficiently query which objects are new.
	quarantinedRepo := repo
	if quarantineDir != nil {
		quarantinedRepo = quarantineDir.QuarantinedRepo()
	}

	hooksPayload, err := git.NewHooksPayload(u.cfg, quarantinedRepo, transaction, &receiveHooksPayload, git.ReceivePackHooks, featureflag.RawFromContext(ctx)).Env()
	if err != nil {
		return err
	}

	var stdout, stderr bytes.Buffer
	if err := u.hookManager.PreReceiveHook(ctx, quarantinedRepo, pushOptions, []string{hooksPayload}, strings.NewReader(changes), &stdout, &stderr); err != nil {
		msg := hookErrorMessage(stdout.String(), stderr.String(), err)
		return PreReceiveError{Message: msg}
	}

	if err := u.hookManager.UpdateHook(ctx, quarantinedRepo, reference.String(), oldrev.String(), newrev.String(), []string{hooksPayload}, &stdout, &stderr); err != nil {
		msg := hookErrorMessage(stdout.String(), stderr.String(), err)
		return PreReceiveError{Message: msg}
	}

	if err := u.hookManager.ReferenceTransactionHook(ctx, hook.ReferenceTransactionPrepared, []string{hooksPayload}, strings.NewReader(changes)); err != nil {
		return PreReceiveError{Message: err.Error()}
	}

	// Now that Rails has told us that the change is okay via the pre-receive hook and where
	// transactional voting via the reference-transaction hook also passed, we can migrate any
	// potentially quarantined objects into the main repository.
	if quarantineDir != nil {
		if err := quarantineDir.Migrate(); err != nil {
			return fmt.Errorf("migrating quarantined objects: %w", err)
		}

		// We only need to update the hooks payload to the unquarantined repo in case we
		// had a quarantine environment. Otherwise, the initial hooks payload is for the
		// real repository anyway.
		hooksPayload, err = git.NewHooksPayload(u.cfg, repo, transaction, &receiveHooksPayload, git.ReceivePackHooks, featureflag.RawFromContext(ctx)).Env()
		if err != nil {
			return err
		}
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
	updater, err := New(ctx, u.cfg, u.localrepo(repo), WithDisabledTransactions())
	if err != nil {
		return err
	}

	if err := updater.Update(reference, newrev.String(), oldrev.String()); err != nil {
		return err
	}

	if err := updater.Wait(); err != nil {
		return Error{reference: reference.String()}
	}

	if err := u.hookManager.ReferenceTransactionHook(ctx, hook.ReferenceTransactionCommitted, []string{hooksPayload}, strings.NewReader(changes)); err != nil {
		return PreReceiveError{Message: err.Error()}
	}

	if err := u.hookManager.PostReceiveHook(ctx, repo, pushOptions, []string{hooksPayload}, strings.NewReader(changes), &stdout, &stderr); err != nil {
		msg := hookErrorMessage(stdout.String(), stderr.String(), err)
		return PreReceiveError{Message: msg}
	}

	return nil
}

func (u *UpdaterWithHooks) localrepo(repo repository.GitRepo) *localrepo.Repo {
	return localrepo.New(u.gitCmdFactory, u.catfileCache, repo, u.cfg)
}
