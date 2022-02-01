package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

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
	// CommandOptions is a list of options to use with 'git' command.
	CommandOptions []git.CmdOpt
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
	// DisableTransactions will disable the reference-transaction hook and atomic transactions.
	DisableTransactions bool
}

// ErrFetchFailed indicates that the fetch has failed.
type ErrFetchFailed struct {
	err error
}

// Error returns the error message.
func (e ErrFetchFailed) Error() string {
	return e.err.Error()
}

// FetchRemote fetches changes from the specified remote. Returns an ErrFetchFailed error in case
// the fetch itself failed.
func (repo *Repo) FetchRemote(ctx context.Context, remoteName string, opts FetchOpts) error {
	if err := validateNotBlank(remoteName, "remoteName"); err != nil {
		return err
	}

	var stderr bytes.Buffer
	if opts.Stderr == nil {
		opts.Stderr = &stderr
	}

	commandOptions := []git.CmdOpt{
		git.WithEnv(opts.Env...),
		git.WithStderr(opts.Stderr),
	}
	if opts.DisableTransactions {
		commandOptions = append(commandOptions, git.WithDisabledHooks())
	} else {
		commandOptions = append(commandOptions, git.WithRefTxHook(repo))
	}
	commandOptions = append(commandOptions, opts.CommandOptions...)

	cmd, err := repo.gitCmdFactory.New(ctx, repo,
		git.SubCmd{
			Name:  "fetch",
			Flags: opts.buildFlags(),
			Args:  []string{remoteName},
		},
		commandOptions...,
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return ErrFetchFailed{errorWithStderr(err, stderr.Bytes())}
	}

	return nil
}

var replicationUploadPackTimeoutSeconds = 30 * 60 // 30 minutes

// FetchInternal performs a fetch from an internal Gitaly-hosted repository. Returns an
// ErrFetchFailed error in case git-fetch(1) failed.
func (repo *Repo) FetchInternal(
	ctx context.Context,
	remoteRepo *gitalypb.Repository,
	refspecs []string,
	opts FetchOpts,
) error {
	if len(refspecs) == 0 {
		return fmt.Errorf("fetch internal called without refspecs")
	}

	var stderr bytes.Buffer
	if opts.Stderr == nil {
		opts.Stderr = &stderr
	}

	commandOptions := []git.CmdOpt{
		git.WithInternalFetch(&gitalypb.SSHUploadPackRequest{
			Repository:       remoteRepo,
			GitConfigOptions: []string{"uploadpack.allowAnySHA1InWant=true"},
			TimeoutSeconds:   int32(replicationUploadPackTimeoutSeconds),
		}),
		git.WithEnv(opts.Env...),
		git.WithStderr(opts.Stderr),
		// We've observed performance issues when fetching into big repositories part of an
		// object pool. The root cause of this seems to be the connectivity check, which by
		// default will also include references of any alternates. Given that object pools
		// often have hundreds of thousands of references, this is quite expensive to
		// compute. Below config entry will disable listing of alternate refs: they
		// shouldn't even be included in the negotiation phase, so they aren't going to
		// matter in the connectivity check either.
		git.WithConfig(git.ConfigPair{Key: "core.alternateRefsCommand", Value: "exit 0 #"}),
	}
	if opts.DisableTransactions {
		commandOptions = append(commandOptions, git.WithDisabledHooks())
	} else {
		commandOptions = append(commandOptions, git.WithRefTxHook(repo))
	}
	commandOptions = append(commandOptions, opts.CommandOptions...)

	if err := repo.ExecAndWait(ctx,
		git.SubCmd{
			Name:  "fetch",
			Flags: opts.buildFlags(),
			Args:  append([]string{git.InternalGitalyURL}, refspecs...),
		},
		commandOptions...,
	); err != nil {
		return ErrFetchFailed{errorWithStderr(err, stderr.Bytes())}
	}

	return nil
}

func (opts FetchOpts) buildFlags() []git.Option {
	flags := []git.Option{}

	if !opts.Verbose {
		flags = append(flags, git.Flag{Name: "--quiet"})
	}

	if opts.Prune {
		flags = append(flags, git.Flag{Name: "--prune"})
	}

	if opts.Force {
		flags = append(flags, git.Flag{Name: "--force"})
	}

	if opts.Tags != FetchOptsTagsDefault {
		flags = append(flags, git.Flag{Name: opts.Tags.String()})
	}

	if !opts.DisableTransactions {
		flags = append(flags, git.Flag{Name: "--atomic"})
	}

	return flags
}

func validateNotBlank(val, name string) error {
	if strings.TrimSpace(val) == "" {
		return fmt.Errorf("%w: %q is blank or empty", git.ErrInvalidArg, name)
	}
	return nil
}

func envGitSSHCommand(cmd string) string {
	return "GIT_SSH_COMMAND=" + cmd
}

// PushOptions are options that can be configured for a push.
type PushOptions struct {
	// SSHCommand is the command line to use for git's SSH invocation. The command line is used
	// as is and must be verified by the caller to be safe.
	SSHCommand string
	// Force decides whether to force push all of the refspecs.
	Force bool
	// Config is the Git configuration which gets passed to the git-push(1) invocation.
	// Configuration is set up via `WithConfigEnv()`, so potential credentials won't be leaked
	// via the command line.
	Config []git.ConfigPair
}

// Push force pushes the refspecs to the remote.
func (repo *Repo) Push(ctx context.Context, remote string, refspecs []string, options PushOptions) error {
	if len(refspecs) == 0 {
		return errors.New("refspecs to push must be explicitly specified")
	}

	var env []string
	if options.SSHCommand != "" {
		env = append(env, envGitSSHCommand(options.SSHCommand))
	}

	var flags []git.Option
	if options.Force {
		flags = append(flags, git.Flag{Name: "--force"})
	}

	stderr := &bytes.Buffer{}
	if err := repo.ExecAndWait(ctx,
		git.SubCmd{
			Name:  "push",
			Flags: flags,
			Args:  append([]string{remote}, refspecs...),
		},
		git.WithStderr(stderr),
		git.WithEnv(env...),
		git.WithConfigEnv(options.Config...),
	); err != nil {
		return fmt.Errorf("git push: %w, stderr: %q", err, stderr)
	}

	return nil
}
