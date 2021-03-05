package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

var (
	// ErrObjectNotFound is returned in case an object could not be found.
	ErrObjectNotFound = errors.New("object not found")
)

// WriteBlob writes a blob to the repository's object database and
// returns its object ID. Path is used by git to decide which filters to
// run on the content.
func (repo *Repo) WriteBlob(ctx context.Context, path string, content io.Reader) (git.ObjectID, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	cmd, err := repo.Exec(ctx, nil,
		git.SubCmd{
			Name: "hash-object",
			Flags: []git.Option{
				git.ValueFlag{Name: "--path", Value: path},
				git.Flag{Name: "--stdin"},
				git.Flag{Name: "-w"},
			},
		},
		git.WithStdin(content),
		git.WithStdout(stdout),
		git.WithStderr(stderr),
	)
	if err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", errorWithStderr(err, stderr.Bytes())
	}

	oid, err := git.NewObjectIDFromHex(text.ChompBytes(stdout.Bytes()))
	if err != nil {
		return "", err
	}

	return oid, nil
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
func FormatTag(
	objectID git.ObjectID,
	objectType string,
	tagName, userName, userEmail, tagBody []byte,
	committerDate time.Time,
) (string, error) {
	if committerDate.IsZero() {
		committerDate = time.Now()
	}

	tagHeaderFormat := "object %s\n" +
		"type %s\n" +
		"tag %s\n" +
		"tagger %s <%s> %d +0000\n"
	tagBuf := fmt.Sprintf(tagHeaderFormat, objectID.String(), objectType, tagName, userName, userEmail, committerDate.Unix())

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
func (repo *Repo) WriteTag(
	ctx context.Context,
	objectID git.ObjectID,
	objectType string,
	tagName, userName, userEmail, tagBody []byte,
	committerDate time.Time,
) (string, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	tagBuf, err := FormatTag(objectID, objectType, tagName, userName, userEmail, tagBody, committerDate)
	if err != nil {
		return "", err
	}

	content := strings.NewReader(tagBuf)

	cmd, err := repo.Exec(ctx, nil,
		git.SubCmd{
			Name: "mktag",
		},
		git.WithStdin(content),
		git.WithStdout(stdout),
		git.WithStderr(stderr),
	)
	if err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", MktagError{tagName: tagName, stderr: stderr.String()}
	}

	return text.ChompBytes(stdout.Bytes()), nil
}

// InvalidObjectError is returned when trying to get an object id that is invalid or does not exist.
type InvalidObjectError string

func (err InvalidObjectError) Error() string { return fmt.Sprintf("invalid object %q", string(err)) }

// ReadObject reads an object from the repository's object database. InvalidObjectError
// is returned if the oid does not refer to a valid object.
func (repo *Repo) ReadObject(ctx context.Context, oid git.ObjectID) ([]byte, error) {
	const msgInvalidObject = "fatal: Not a valid object name "

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd, err := repo.Exec(ctx, nil,
		git.SubCmd{
			Name:  "cat-file",
			Flags: []git.Option{git.Flag{"-p"}},
			Args:  []string{oid.String()},
		},
		git.WithStdout(stdout),
		git.WithStderr(stderr),
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

type readCommitConfig struct {
	withTrailers bool
}

// ReadCommitOpt is an option for ReadCommit.
type ReadCommitOpt func(*readCommitConfig)

// WithTrailers will cause ReadCommit to parse commit trailers.
func WithTrailers() ReadCommitOpt {
	return func(cfg *readCommitConfig) {
		cfg.withTrailers = true
	}
}

// ReadCommit reads the commit specified by the given revision. If no such
// revision exists, it will return an ErrObjectNotFound error.
func (repo *Repo) ReadCommit(ctx context.Context, revision git.Revision, opts ...ReadCommitOpt) (*gitalypb.GitCommit, error) {
	var cfg readCommitConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	c, err := catfile.New(ctx, repo.gitCmdFactory, repo)
	if err != nil {
		return nil, err
	}

	var commit *gitalypb.GitCommit
	if cfg.withTrailers {
		commit, err = log.GetCommitCatfileWithTrailers(ctx, repo.gitCmdFactory, repo, c, revision)
	} else {
		commit, err = log.GetCommitCatfile(ctx, c, revision)
	}

	if err != nil {
		if log.IsNotFound(err) {
			return nil, ErrObjectNotFound
		}
		return nil, err
	}

	return commit, nil
}
