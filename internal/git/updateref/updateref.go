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

	cmd, err := git.NewCommand(ctx, repo, nil,
		git.SubCmd{
			Name:  "update-ref",
			Flags: []git.Option{git.Flag{Name: "-z"}, git.Flag{Name: "--stdin"}},
		},
		txOption,
		git.WithStdin(command.SetupStdin),
	)
	if err != nil {
		return nil, err
	}

	// By writing an explicit "start" to the command, we enable
	// transactional behaviour. Which effectively means that without an
	// explicit "commit", no changes will be inadvertently committed to
	// disk.
	fmt.Fprintf(cmd, "start\x00")

	return &Updater{repo: repo, cmd: cmd}, nil
}

// Create commands the reference to be created with the sha specified in value
func (u *Updater) Create(reference git.ReferenceName, value string) error {
	_, err := fmt.Fprintf(u.cmd, "create %s\x00%s\x00", reference.String(), value)
	return err
}

// Update commands the reference to be updated to point at the sha specified in
// newvalue
func (u *Updater) Update(reference git.ReferenceName, newvalue, oldvalue string) error {
	_, err := fmt.Fprintf(u.cmd, "update %s\x00%s\x00%s\x00", reference.String(), newvalue, oldvalue)
	return err
}

// Delete commands the reference to be removed from the repository
func (u *Updater) Delete(reference git.ReferenceName) error {
	_, err := fmt.Fprintf(u.cmd, "delete %s\x00\x00", reference.String())
	return err
}

// Wait applies the commands specified in other calls to the Updater
func (u *Updater) Wait() error {
	fmt.Fprintf(u.cmd, "commit\x00")

	if err := u.cmd.Wait(); err != nil {
		return fmt.Errorf("git update-ref: %v", err)
	}

	return nil
}
