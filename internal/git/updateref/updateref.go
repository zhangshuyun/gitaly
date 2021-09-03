package updateref

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// Updater wraps a `git update-ref --stdin` process, presenting an interface
// that allows references to be easily updated in bulk. It is not suitable for
// concurrent use.
type Updater struct {
	repo   git.RepositoryExecutor
	cmd    *command.Command
	stderr *bytes.Buffer
}

// UpdaterOpt is a type representing options for the Updater.
type UpdaterOpt func(*updaterConfig)

type updaterConfig struct {
	disableTransactions bool
}

// WithDisabledTransactions disables hooks such that no reference-transactions
// are used for the updater.
func WithDisabledTransactions() UpdaterOpt {
	return func(cfg *updaterConfig) {
		cfg.disableTransactions = true
	}
}

// New returns a new bulk updater, wrapping a `git update-ref` process. Call the
// various methods to enqueue updates, then call Commit() to attempt to apply all
// the updates at once.
//
// It is important that ctx gets canceled somewhere. If it doesn't, the process
// spawned by New() may never terminate.
func New(ctx context.Context, conf config.Cfg, repo git.RepositoryExecutor, opts ...UpdaterOpt) (*Updater, error) {
	var cfg updaterConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	txOption := git.WithRefTxHook(ctx, repo, conf)
	if cfg.disableTransactions {
		txOption = git.WithDisabledHooks()
	}

	var stderr bytes.Buffer
	cmd, err := repo.Exec(ctx,
		git.SubCmd{
			Name:  "update-ref",
			Flags: []git.Option{git.Flag{Name: "-z"}, git.Flag{Name: "--stdin"}},
		},
		txOption,
		git.WithStdin(command.SetupStdin),
		git.WithStderr(&stderr),
	)
	if err != nil {
		return nil, err
	}

	updater := &Updater{
		repo:   repo,
		cmd:    cmd,
		stderr: &stderr,
	}

	// By writing an explicit "start" to the command, we enable
	// transactional behaviour. Which effectively means that without an
	// explicit "commit", no changes will be inadvertently committed to
	// disk.
	if err := updater.setState("start"); err != nil {
		return nil, err
	}

	return updater, nil
}

// Update commands the reference to be updated to point at the object ID specified in newvalue. If
// newvalue is the zero OID, then the branch will be deleted. If oldvalue is a non-empty string,
// then the reference will only be updated if its current value matches the old value. If the old
// value is the zero OID, then the branch must not exist.
func (u *Updater) Update(reference git.ReferenceName, newvalue, oldvalue string) error {
	_, err := fmt.Fprintf(u.cmd, "update %s\x00%s\x00%s\x00", reference.String(), newvalue, oldvalue)
	return err
}

// Create commands the reference to be created with the given object ID. The ref must not exist.
func (u *Updater) Create(reference git.ReferenceName, value string) error {
	return u.Update(reference, value, git.ZeroOID.String())
}

// Delete commands the reference to be removed from the repository. This command will ignore any old
// state of the reference and just force-remove it.
func (u *Updater) Delete(reference git.ReferenceName) error {
	return u.Update(reference, git.ZeroOID.String(), "")
}

// Prepare prepares the reference transaction by locking all references and determining their
// current values. The updates are not yet committed and will be rolled back in case there is no
// call to `Commit()`. This call is optional.
func (u *Updater) Prepare() error {
	return u.setState("prepare")
}

// Commit applies the commands specified in other calls to the Updater
func (u *Updater) Commit() error {
	if err := u.setState("commit"); err != nil {
		return err
	}

	if err := u.cmd.Wait(); err != nil {
		return fmt.Errorf("git update-ref: %v, stderr: %q", err, u.stderr)
	}

	return nil
}

// Cancel aborts the transaction. No changes will be written to disk, all lockfiles will be cleaned
// up and the process will exit.
func (u *Updater) Cancel() error {
	if err := u.cmd.Wait(); err != nil {
		return fmt.Errorf("canceling update: %w", err)
	}
	return nil
}

func (u *Updater) setState(state string) error {
	_, err := fmt.Fprintf(u.cmd, "%s\x00", state)
	if err != nil {
		return fmt.Errorf("updating state to %s: %w", state, err)
	}

	// For each state-changing command, git-update-ref(1) will report successful execution via
	// "<command>: ok" lines printed to its stdout. Ideally, we should thus verify here whether
	// the command was successfully executed by checking for exactly this line, otherwise we
	// cannot be sure whether the command has correctly been processed by Git or if an error was
	// raised. Unfortunately, Git won't flush the output though, and as such we would be left
	// waiting for the message to appear here. This needs to be fixed upstream first, and after
	// this we should adapt this function to assert commands.

	return nil
}
