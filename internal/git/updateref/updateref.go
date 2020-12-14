package updateref

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// Updater wraps a `git update-ref --stdin` process, presenting an interface
// that allows references to be easily updated in bulk. It is not suitable for
// concurrent use.
type Updater struct {
	repo repository.GitRepo
	cmd  *command.Command
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
// various methods to enqueue updates, then call Wait() to attempt to apply all
// the updates at once.
//
// It is important that ctx gets canceled somewhere. If it doesn't, the process
// spawned by New() may never terminate.
func New(ctx context.Context, repo repository.GitRepo, opts ...UpdaterOpt) (*Updater, error) {
	var cfg updaterConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	txOption := git.WithRefTxHook(ctx, helper.ProtoRepoFromRepo(repo), config.Config)
	if cfg.disableTransactions {
		txOption = git.WithDisabledHooks()
	}

	cmd, err := git.SafeStdinCmd(ctx, repo, nil,
		git.SubCmd{
			Name:  "update-ref",
			Flags: []git.Option{git.Flag{Name: "-z"}, git.Flag{Name: "--stdin"}},
		},
		txOption,
	)
	if err != nil {
		return nil, err
	}

	return &Updater{repo: repo, cmd: cmd}, nil
}

// CreateUpdateDelete dispatches to Create()/Update()/Delete() as needed
//
// The underlying "update-ref" does the same dispatching depending on
// what it get passed.
func (u *Updater) CreateUpdateDelete(ref, newvalue, oldvalue string) error {
	if newvalue == git.NullSHA {
		return u.Delete(ref, oldvalue)
	} else if oldvalue == git.NullSHA {
		return u.Create(ref, newvalue)
	} else {
		return u.Update(ref, newvalue, oldvalue)
	}
}

// Create commands the reference to be created with the sha specified in value
func (u *Updater) Create(ref, value string) error {
	_, err := fmt.Fprintf(u.cmd, "create %s\x00%s\x00", ref, value)
	return err
}

// Update commands the reference to be updated to point at the sha specified in
// newvalue
func (u *Updater) Update(ref, newvalue, oldvalue string) error {
	_, err := fmt.Fprintf(u.cmd, "update %s\x00%s\x00%s\x00", ref, newvalue, oldvalue)
	return err
}

// Delete commands the reference to be removed from the repository
func (u *Updater) Delete(ref string, oldvalue string) error {
	_, err := fmt.Fprintf(u.cmd, "delete %s\x00%s\x00", ref, oldvalue)
	return err
}

// Wait applies the commands specified in other calls to the Updater
func (u *Updater) Wait() error {
	if err := u.cmd.Wait(); err != nil {
		return fmt.Errorf("git update-ref: %v", err)
	}

	return nil
}
