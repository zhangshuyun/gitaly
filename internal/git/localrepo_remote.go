package git

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// LocalRepositoryRemote provides functionality of the 'remote' git sub-command.
type LocalRepositoryRemote struct {
	repo repository.GitRepo
}

// Add adds a new remote to the repository.
func (repo LocalRepositoryRemote) Add(ctx context.Context, name, url string, opts RemoteAddOpts) error {
	if err := validateNotBlank(name, "name"); err != nil {
		return err
	}

	if err := validateNotBlank(url, "url"); err != nil {
		return err
	}

	stderr := bytes.Buffer{}
	cmd, err := NewCommand(ctx, repo.repo, nil,
		SubSubCmd{
			Name:   "remote",
			Action: "add",
			Flags:  opts.buildFlags(),
			Args:   []string{name, url},
		},
		WithStderr(&stderr),
		WithRefTxHook(ctx, helper.ProtoRepoFromRepo(repo.repo), config.Config),
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		status, ok := command.ExitStatus(err)
		if !ok {
			return err
		}

		if status == 3 {
			// In Git v2.30.0 and newer (https://gitlab.com/git-vcs/git/commit/9144ba4cf52)
			return ErrAlreadyExists
		}
		if status == 128 && bytes.HasPrefix(stderr.Bytes(), []byte("fatal: remote "+name+" already exists")) {
			// ..in older versions we parse stderr
			return ErrAlreadyExists
		}
	}

	return nil
}

// Remove removes a named remote from the repository configuration.
func (repo LocalRepositoryRemote) Remove(ctx context.Context, name string) error {
	if err := validateNotBlank(name, "name"); err != nil {
		return err
	}

	var stderr bytes.Buffer
	cmd, err := NewCommand(ctx, repo.repo, nil,
		SubSubCmd{
			Name:   "remote",
			Action: "remove",
			Args:   []string{name},
		},
		WithStderr(&stderr),
		WithRefTxHook(ctx, helper.ProtoRepoFromRepo(repo.repo), config.Config),
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		status, ok := command.ExitStatus(err)
		if !ok {
			return err
		}

		if status == 2 {
			// In Git v2.30.0 and newer (https://gitlab.com/git-vcs/git/commit/9144ba4cf52)
			return ErrNotFound
		}
		if status == 128 && strings.HasPrefix(stderr.String(), "fatal: No such remote") {
			// ..in older versions we parse stderr
			return ErrNotFound
		}
	}

	return err
}

// SetURL sets the URL for a given remote.
func (repo LocalRepositoryRemote) SetURL(ctx context.Context, name, url string, opts SetURLOpts) error {
	if err := validateNotBlank(name, "name"); err != nil {
		return err
	}

	if err := validateNotBlank(url, "url"); err != nil {
		return err
	}

	var stderr bytes.Buffer
	cmd, err := NewCommand(ctx, repo.repo, nil,
		SubSubCmd{
			Name:   "remote",
			Action: "set-url",
			Flags:  opts.buildFlags(),
			Args:   []string{name, url},
		},
		WithStderr(&stderr),
		WithRefTxHook(ctx, helper.ProtoRepoFromRepo(repo.repo), config.Config),
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		status, ok := command.ExitStatus(err)
		if !ok {
			return err
		}

		if status == 2 {
			// In Git v2.30.0 and newer (https://gitlab.com/git-vcs/git/commit/9144ba4cf52)
			return ErrNotFound
		}
		if status == 128 && strings.HasPrefix(stderr.String(), "fatal: No such remote") {
			// ..in older versions we parse stderr
			return ErrNotFound
		}
	}

	return err
}

// FetchOptsTags controls what tags needs to be imported on fetch.
type FetchOptsTags string

func (t FetchOptsTags) String() string {
	return string(t)
}

var (
	// FetchOptsTagsDefault enables importing of tags only on fetched branches.
	FetchOptsTagsDefault = FetchOptsTags("")
	// FetchOptsTagsAll enables importing of every tag from the remote repository.
	FetchOptsTagsAll = FetchOptsTags("--tags")
	// FetchOptsTagsNone disables importing of tags from the remote repository.
	FetchOptsTagsNone = FetchOptsTags("--no-tags")
)

// FetchOpts is used to configure invocation of the 'FetchRemote' command.
type FetchOpts struct {
	// Env is a list of env vars to pass to the cmd.
	Env []string
	// Global is a list of global flags to use with 'git' command.
	Global []GlobalOption
	// Prune if set fetch removes any remote-tracking references that no longer exist on the remote.
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---prune
	Prune bool
	// Force if set fetch overrides local references with values from remote that's
	// doesn't have the previous commit as an ancestor.
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---force
	Force bool
	// Verbose controls how much information is written to stderr. The list of
	// refs updated by the fetch will only be listed if verbose is true.
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---quiet
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---verbose
	Verbose bool
	// Tags controls whether tags will be fetched as part of the remote or not.
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---tags
	// https://git-scm.com/docs/git-fetch#Documentation/git-fetch.txt---no-tags
	Tags FetchOptsTags
	// Stderr if set it would be used to redirect stderr stream into it.
	Stderr io.Writer
}

// FetchRemote fetches changes from the specified remote.
func (repo *LocalRepository) FetchRemote(ctx context.Context, remoteName string, opts FetchOpts) error {
	if err := validateNotBlank(remoteName, "remoteName"); err != nil {
		return err
	}

	cmd, err := NewCommand(ctx, repo.repo, opts.Global,
		SubCmd{
			Name:  "fetch",
			Flags: opts.buildFlags(),
			Args:  []string{remoteName},
		},
		WithEnv(opts.Env...),
		WithStderr(opts.Stderr),
		WithDisabledHooks(),
	)
	if err != nil {
		return err
	}

	return cmd.Wait()
}

func (opts FetchOpts) buildFlags() []Option {
	flags := []Option{}

	if !opts.Verbose {
		flags = append(flags, Flag{Name: "--quiet"})
	}

	if opts.Prune {
		flags = append(flags, Flag{Name: "--prune"})
	}

	if opts.Force {
		flags = append(flags, Flag{Name: "--force"})
	}

	if opts.Tags != FetchOptsTagsDefault {
		flags = append(flags, Flag{Name: opts.Tags.String()})
	}

	return flags
}

func validateNotBlank(val, name string) error {
	if strings.TrimSpace(val) == "" {
		return fmt.Errorf("%w: %q is blank or empty", ErrInvalidArg, name)
	}
	return nil
}
