package git

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
)

// InvalidObjectError is returned when trying to get an object id that is invalid or does not exist.
type InvalidObjectError string

func (err InvalidObjectError) Error() string { return fmt.Sprintf("invalid object %q", string(err)) }

func errorWithStderr(err error, stderr []byte) error {
	return fmt.Errorf("%w, stderr: %q", err, stderr)
}

var (
	ErrReferenceNotFound = errors.New("reference not found")
	// ErrAlreadyExists represents an error when the resource is already exists.
	ErrAlreadyExists = errors.New("already exists")
	// ErrNotFound represents an error when the resource can't be found.
	ErrNotFound = errors.New("not found")
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

// Repository is the common interface of different repository implementations.
type Repository interface {
	// ResolveRevision tries to resolve the given revision to its object
	// ID. This uses the typical DWIM mechanism of git, see gitrevisions(1)
	// for accepted syntax. This will not verify whether the object ID
	// exists. To do so, you can peel the reference to a given object type,
	// e.g. by passing `refs/heads/master^{commit}`.
	ResolveRevision(ctx context.Context, revision Revision) (ObjectID, error)
	// HasBranches returns whether the repository has branches.
	HasBranches(ctx context.Context) (bool, error)
}

// LocalRepository represents a local Git repository.
type LocalRepository struct {
	repo           repository.GitRepo
	commandFactory *ExecCommandFactory
	cfg            config.Cfg
}

// NewRepository creates a new Repository from its protobuf representation.
func NewRepository(repo repository.GitRepo, cfg config.Cfg) *LocalRepository {
	return &LocalRepository{
		repo:           repo,
		cfg:            cfg,
		commandFactory: NewExecCommandFactory(cfg),
	}
}

// command creates a Git Command with the given args and Repository, executed
// in the Repository. It validates the arguments in the command before
// executing.
func (repo *LocalRepository) command(ctx context.Context, globals []GlobalOption, cmd SubCmd, opts ...CmdOpt) (*command.Command, error) {
	return repo.commandFactory.newCommand(ctx, repo.repo, "", globals, cmd, opts...)
}

// WriteBlob writes a blob to the repository's object database and
// returns its object ID. Path is used by git to decide which filters to
// run on the content.
func (repo *LocalRepository) WriteBlob(ctx context.Context, path string, content io.Reader) (string, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	cmd, err := repo.command(ctx, nil,
		SubCmd{
			Name: "hash-object",
			Flags: []Option{
				ValueFlag{Name: "--path", Value: path},
				Flag{Name: "--stdin"}, Flag{Name: "-w"},
			},
		},
		WithStdin(content),
		WithStdout(stdout),
		WithStderr(stderr),
	)
	if err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", errorWithStderr(err, stderr.Bytes())
	}

	return text.ChompBytes(stdout.Bytes()), nil
}

// FormatTagError is used by FormatTag() below
type FormatTagError struct {
	expectedLines int
	actualLines   int
}

func (e FormatTagError) Error() string {
	return fmt.Sprintf("should have %d tag header lines, got %d", e.expectedLines, e.actualLines)
}

// FormatTag is used by WriteTag (or for testing) to make the tag
// signature to feed to git-mktag, i.e. the plain-text mktag
// format. This does not create an object, just crafts input for "git
// mktag" to consume.
//
// We are not being paranoid about exhaustive input validation here
// because we're just about to run git's own "fsck" check on this.
//
// However, if someone injected parameters with extra newlines they
// could cause subsequent values to be ignore via a crafted
// message. This someone could also locally craft a tag locally and
// "git push" it. But allowing e.g. someone to provide their own
// timestamp here would at best be annoying, and at worst run up
// against some other assumption (e.g. that some hook check isn't as
// strict on locally generated data).
func FormatTag(objectID, objectType string, tagName, userName, userEmail, tagBody []byte) (string, error) {
	unixEpoch := time.Now().Unix()
	tagHeaderFormat := "object %s\n" +
		"type %s\n" +
		"tag %s\n" +
		"tagger %s <%s> %d +0000\n"
	tagBuf := fmt.Sprintf(tagHeaderFormat, objectID, objectType, tagName, userName, userEmail, unixEpoch)

	maxHeaderLines := 4
	actualHeaderLines := strings.Count(tagBuf, "\n")
	if actualHeaderLines != maxHeaderLines {
		return "", FormatTagError{expectedLines: maxHeaderLines, actualLines: actualHeaderLines}
	}

	tagBuf += "\n"
	tagBuf += string(tagBody)

	return tagBuf, nil
}

// MktagError is used by WriteTag() below
type MktagError struct {
	tagName []byte
	stderr  string
}

func (e MktagError) Error() string {
	// TODO: Upper-case error message purely for transitory backwards compatibility
	return fmt.Sprintf("Could not update refs/tags/%s. Please refresh and try again.", e.tagName)
}

// WriteTag writes a tag to the repository's object database with
// git-mktag and returns its object ID.
//
// It's important that this be git-mktag and not git-hash-object due
// to its fsck sanity checking semantics.
func (repo *LocalRepository) WriteTag(ctx context.Context, objectID, objectType string, tagName, userName, userEmail, tagBody []byte) (string, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	tagBuf, err := FormatTag(objectID, objectType, tagName, userName, userEmail, tagBody)
	if err != nil {
		return "", err
	}

	content := strings.NewReader(tagBuf)

	cmd, err := repo.command(ctx, nil,
		SubCmd{
			Name: "mktag",
		},
		WithStdin(content),
		WithStdout(stdout),
		WithStderr(stderr),
	)
	if err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", MktagError{tagName: tagName, stderr: stderr.String()}
	}

	return text.ChompBytes(stdout.Bytes()), nil
}

// ReadObject reads an object from the repository's object database. InvalidObjectError
// is returned if the oid does not refer to a valid object.
func (repo *LocalRepository) ReadObject(ctx context.Context, oid string) ([]byte, error) {
	const msgInvalidObject = "fatal: Not a valid object name "

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd, err := repo.command(ctx, nil,
		SubCmd{
			Name:  "cat-file",
			Flags: []Option{Flag{"-p"}},
			Args:  []string{oid},
		},
		WithStdout(stdout),
		WithStderr(stderr),
	)
	if err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		msg := text.ChompBytes(stderr.Bytes())
		if strings.HasPrefix(msg, msgInvalidObject) {
			return nil, InvalidObjectError(strings.TrimPrefix(msg, msgInvalidObject))
		}

		return nil, errorWithStderr(err, stderr.Bytes())
	}

	return stdout.Bytes(), nil
}

// ResolveRevision resolves the given revision to its object ID. This will not
// verify whether the target object exists. To do so, you can peel the
// reference to a given object type, e.g. by passing
// `refs/heads/master^{commit}`. Returns an ErrReferenceNotFound error in case
// the revision does not exist.
func (repo *LocalRepository) ResolveRevision(ctx context.Context, revision Revision) (ObjectID, error) {
	if revision.String() == "" {
		return "", errors.New("repository cannot contain empty reference name")
	}

	cmd, err := repo.command(ctx, nil, SubCmd{
		Name:  "rev-parse",
		Flags: []Option{Flag{Name: "--verify"}},
		Args:  []string{revision.String()},
	}, WithStderr(ioutil.Discard))
	if err != nil {
		return "", err
	}

	var stdout bytes.Buffer
	io.Copy(&stdout, cmd)

	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return "", ErrReferenceNotFound
		}
		return "", err
	}

	hex := strings.TrimSpace(stdout.String())
	oid, err := NewObjectIDFromHex(hex)
	if err != nil {
		return "", fmt.Errorf("unsupported object hash %q: %w", hex, err)
	}

	return oid, nil
}

// HasRevision checks if a revision in the repository exists. This will not
// verify whether the target object exists. To do so, you can peel the revision
// to a given object type, e.g. by passing `refs/heads/master^{commit}`.
func (repo *LocalRepository) HasRevision(ctx context.Context, revision Revision) (bool, error) {
	if _, err := repo.ResolveRevision(ctx, revision); err != nil {
		if errors.Is(err, ErrReferenceNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetReference looks up and returns the given reference. Returns a
// ReferenceNotFound error if the reference was not found.
func (repo *LocalRepository) GetReference(ctx context.Context, reference ReferenceName) (Reference, error) {
	refs, err := repo.GetReferences(ctx, reference.String())
	if err != nil {
		return Reference{}, err
	}

	if len(refs) == 0 {
		return Reference{}, ErrReferenceNotFound
	}

	return refs[0], nil
}

func (repo *LocalRepository) HasBranches(ctx context.Context) (bool, error) {
	refs, err := repo.getReferences(ctx, "refs/heads/", 1)
	return len(refs) > 0, err
}

// GetReferences returns references matching the given pattern.
func (repo *LocalRepository) GetReferences(ctx context.Context, pattern string) ([]Reference, error) {
	return repo.getReferences(ctx, pattern, 0)
}

func (repo *LocalRepository) getReferences(ctx context.Context, pattern string, limit uint) ([]Reference, error) {
	flags := []Option{Flag{Name: "--format=%(refname)%00%(objectname)%00%(symref)"}}
	if limit > 0 {
		flags = append(flags, Flag{Name: fmt.Sprintf("--count=%d", limit)})
	}

	var args []string
	if pattern != "" {
		args = []string{pattern}
	}

	cmd, err := repo.command(ctx, nil, SubCmd{
		Name:  "for-each-ref",
		Flags: flags,
		Args:  args,
	})
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(cmd)

	var refs []Reference
	for scanner.Scan() {
		line := bytes.SplitN(scanner.Bytes(), []byte{0}, 3)
		if len(line) != 3 {
			return nil, errors.New("unexpected reference format")
		}

		if len(line[2]) == 0 {
			refs = append(refs, NewReference(ReferenceName(line[0]), string(line[1])))
		} else {
			refs = append(refs, NewSymbolicReference(ReferenceName(line[0]), string(line[1])))
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading standard input: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return refs, nil
}

// GetBranches returns all branches.
func (repo *LocalRepository) GetBranches(ctx context.Context) ([]Reference, error) {
	return repo.GetReferences(ctx, "refs/heads/")
}

// UpdateRef updates reference from oldValue to newValue. If oldValue is a
// non-empty string, the update will fail it the reference is not currently at
// that revision. If newValue is the ZeroOID, the reference will be deleted.
// If oldValue is the ZeroOID, the reference will created.
func (repo *LocalRepository) UpdateRef(ctx context.Context, reference ReferenceName, newValue, oldValue ObjectID) error {
	cmd, err := repo.command(ctx, nil,
		SubCmd{
			Name:  "update-ref",
			Flags: []Option{Flag{Name: "-z"}, Flag{Name: "--stdin"}},
		},
		WithStdin(strings.NewReader(fmt.Sprintf("update %s\x00%s\x00%s\x00", reference, newValue.String(), oldValue.String()))),
		WithRefTxHook(ctx, helper.ProtoRepoFromRepo(repo.repo), repo.cfg),
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("UpdateRef: failed updating reference %q from %q to %q: %w", reference, newValue, oldValue, err)
	}

	return nil
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

// Config returns executor of the 'config' sub-command.
func (repo *LocalRepository) Config() Config {
	return RepositoryConfig{repo: repo.repo}
}

// Remote returns executor of the 'remote' sub-command.
func (repo *LocalRepository) Remote() Remote {
	return RepositoryRemote{repo: repo.repo}
}

func isExitWithCode(err error, code int) bool {
	actual, ok := command.ExitStatus(err)
	if !ok {
		return false
	}

	return code == actual
}

func validateNotBlank(val, name string) error {
	if strings.TrimSpace(val) == "" {
		return fmt.Errorf("%w: %q is blank or empty", ErrInvalidArg, name)
	}
	return nil
}
