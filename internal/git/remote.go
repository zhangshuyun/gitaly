package git

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// remoteRepository represents a Git repository on a different Gitaly storage
type remoteRepository struct {
	UnimplementedRepo
	repo   *gitalypb.Repository
	server storage.ServerInfo
	pool   *client.Pool
}

// NewRepository creates a new remote Repository from its protobuf representation.
func NewRemoteRepository(ctx context.Context, repo *gitalypb.Repository, pool *client.Pool) (Repository, error) {
	server, err := helper.ExtractGitalyServer(ctx, repo.GetStorageName())
	if err != nil {
		return nil, fmt.Errorf("remote repository: %w", err)
	}

	return remoteRepository{
		repo:   repo,
		server: server,
		pool:   pool,
	}, nil
}

// ResolveRefish will dial to the remote repository and attempt to resolve the
// refish string via the gRPC interface
func (rr remoteRepository) ResolveRefish(ctx context.Context, ref string) (string, error) {
	cc, err := rr.pool.Dial(ctx, rr.server.Address, rr.server.Token)
	if err != nil {
		return "", err
	}

	cli := gitalypb.NewCommitServiceClient(cc)
	resp, err := cli.FindCommit(ctx, &gitalypb.FindCommitRequest{
		Repository: rr.repo,
		Revision:   []byte(ref),
	})
	if err != nil {
		return "", err
	}

	oid := resp.GetCommit().GetId()
	if oid == "" {
		return "", ErrReferenceNotFound
	}

	return oid, nil
}

// Remote represents 'remote' sub-command.
// https://git-scm.com/docs/git-remote
type Remote interface {
	// Add creates a new remote repository if it doesn't exist.
	// If such a remote already exists it returns an ErrAlreadyExists error.
	// https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-emaddem
	Add(ctx context.Context, name, url string, opts RemoteAddOpts) error
	// Remove removes the remote configured for the local repository and all configurations associated with it.
	// https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-emremoveem
	Remove(ctx context.Context, name string) error
	// SetURL sets a new url value for an existing remote.
	// If remote doesn't exist it returns an ErrNotFound error.
	// https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-emset-urlem
	SetURL(ctx context.Context, name, url string, opts SetURLOpts) error
}

// UnimplementedRemote satisfies the Remote interface and used by UnimplementedRepo to reduce friction in
// writing new Repository implementations
type UnimplementedRemote struct{}

func (UnimplementedRemote) Add(context.Context, string, string, RemoteAddOpts) error {
	return ErrUnimplemented
}

func (UnimplementedRemote) Remove(context.Context, string) error {
	return ErrUnimplemented
}

func (UnimplementedRemote) SetURL(context.Context, string, string, SetURLOpts) error {
	return ErrUnimplemented
}

// RepositoryRemote provides functionality of the 'remote' git sub-command.
type RepositoryRemote struct {
	repo repository.GitRepo
}

// RemoteAddOptsMirror represents possible values for the '--mirror' flag value
type RemoteAddOptsMirror string

func (m RemoteAddOptsMirror) String() string {
	return string(m)
}

var (
	// RemoteAddOptsMirrorDefault allows to use a default behaviour.
	RemoteAddOptsMirrorDefault = RemoteAddOptsMirror("")
	// RemoteAddOptsMirrorFetch configures everything in refs/ on the remote to be
	// directly mirrored into refs/ in the local repository.
	RemoteAddOptsMirrorFetch = RemoteAddOptsMirror("fetch")
	// RemoteAddOptsMirrorPush configures 'git push' to always behave as if --mirror was passed.
	RemoteAddOptsMirrorPush = RemoteAddOptsMirror("push")
)

// RemoteAddOptsTags controls whether tags will be fetched.
type RemoteAddOptsTags string

func (t RemoteAddOptsTags) String() string {
	return string(t)
}

var (
	// RemoteAddOptsTagsDefault enables importing of tags only on fetched branches.
	RemoteAddOptsTagsDefault = RemoteAddOptsTags("")
	// RemoteAddOptsTagsAll enables importing of every tag from the remote repository.
	RemoteAddOptsTagsAll = RemoteAddOptsTags("--tags")
	// RemoteAddOptsTagsNone disables importing of tags from the remote repository.
	RemoteAddOptsTagsNone = RemoteAddOptsTags("--no-tags")
)

// RemoteAddOpts is used to configure invocation of the 'git remote add' command.
// https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-emaddem
type RemoteAddOpts struct {
	// RemoteTrackingBranches controls what branches should be tracked instead of
	// all branches which is a default refs/remotes/<name>.
	// For each entry the refspec '+refs/heads/<branch>:refs/remotes/<remote>/<branch>' would be created and added to the configuration.
	RemoteTrackingBranches []string
	// DefaultBranch sets the default branch (i.e. the target of the symbolic-ref refs/remotes/<name>/HEAD)
	// for the named remote.
	// If set to 'develop' then: 'git symbolic-ref refs/remotes/<remote>/HEAD' call will result to 'refs/remotes/<remote>/develop'.
	DefaultBranch string
	// Fetch controls if 'git fetch <name>' is run immediately after the remote information is set up.
	Fetch bool
	// Tags controls whether tags will be fetched as part of the remote or not.
	Tags RemoteAddOptsTags
	// Mirror controls value used for '--mirror' flag.
	Mirror RemoteAddOptsMirror
}

func (opts RemoteAddOpts) buildFlags() []Option {
	var flags []Option
	for _, b := range opts.RemoteTrackingBranches {
		flags = append(flags, ValueFlag{Name: "-t", Value: b})
	}

	if opts.DefaultBranch != "" {
		flags = append(flags, ValueFlag{Name: "-m", Value: opts.DefaultBranch})
	}

	if opts.Fetch {
		flags = append(flags, Flag{Name: "-f"})
	}

	if opts.Tags != RemoteAddOptsTagsDefault {
		flags = append(flags, Flag{Name: opts.Tags.String()})
	}

	if opts.Mirror != RemoteAddOptsMirrorDefault {
		flags = append(flags, ValueFlag{Name: "--mirror", Value: opts.Mirror.String()})
	}

	return flags
}

func (repo RepositoryRemote) Add(ctx context.Context, name, url string, opts RemoteAddOpts) error {
	if err := validateNotBlank(name, "name"); err != nil {
		return err
	}

	if err := validateNotBlank(url, "url"); err != nil {
		return err
	}

	stderr := bytes.Buffer{}
	cmd, err := SafeCmd(ctx, repo.repo, nil,
		SubCmd{
			Name:  "remote",
			Flags: append([]Option{SubSubCmd{Name: "add"}}, opts.buildFlags()...),
			Args:  []string{name, url},
		},
		WithStderr(&stderr),
		WithRefTxHook(ctx, helper.ProtoRepoFromRepo(repo.repo), config.Config),
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		if bytes.Contains(stderr.Bytes(), []byte("remote "+name+" already exists")) {
			return ErrAlreadyExists
		}

		return err
	}

	return nil
}

func (repo RepositoryRemote) Remove(ctx context.Context, name string) error {
	if err := validateNotBlank(name, "name"); err != nil {
		return err
	}

	var stderr bytes.Buffer
	cmd, err := SafeCmd(ctx, repo.repo, nil,
		SubCmd{
			Name:  "remote",
			Flags: []Option{SubSubCmd{Name: "remove"}},
			Args:  []string{name},
		},
		WithStderr(&stderr),
		WithRefTxHook(ctx, helper.ProtoRepoFromRepo(repo.repo), config.Config),
	)
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil && strings.HasPrefix(stderr.String(), "fatal: No such remote") {
		return ErrNotFound
	}

	return err
}

// SetURLOpts are the options for SetURL.
type SetURLOpts struct {
	// Push URLs are manipulated instead of fetch URLs.
	Push bool
}

func (opts SetURLOpts) buildFlags() []Option {
	if opts.Push {
		return []Option{Flag{Name: "--push"}}
	}

	return nil
}

func (repo RepositoryRemote) SetURL(ctx context.Context, name, url string, opts SetURLOpts) error {
	if err := validateNotBlank(name, "name"); err != nil {
		return err
	}

	if err := validateNotBlank(url, "url"); err != nil {
		return err
	}

	var stderr bytes.Buffer
	cmd, err := SafeCmd(ctx, repo.repo, nil,
		SubCmd{
			Name:  "remote",
			Flags: append([]Option{SubSubCmd{Name: "set-url"}}, opts.buildFlags()...),
			Args:  []string{name, url},
		},
		WithStderr(&stderr),
		WithRefTxHook(ctx, helper.ProtoRepoFromRepo(repo.repo), config.Config),
	)
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil && strings.HasPrefix(stderr.String(), "fatal: No such remote") {
		return ErrNotFound
	}

	return err
}
