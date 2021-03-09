package localrepo

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
)

// HasRevision checks if a revision in the repository exists. This will not
// verify whether the target object exists. To do so, you can peel the revision
// to a given object type, e.g. by passing `refs/heads/master^{commit}`.
func (repo *Repo) HasRevision(ctx context.Context, revision git.Revision) (bool, error) {
	if _, err := repo.ResolveRevision(ctx, revision); err != nil {
		if errors.Is(err, git.ErrReferenceNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ResolveRevision resolves the given revision to its object ID. This will not
// verify whether the target object exists. To do so, you can peel the
// reference to a given object type, e.g. by passing
// `refs/heads/master^{commit}`. Returns an ErrReferenceNotFound error in case
// the revision does not exist.
func (repo *Repo) ResolveRevision(ctx context.Context, revision git.Revision) (git.ObjectID, error) {
	if revision.String() == "" {
		return "", errors.New("repository cannot contain empty reference name")
	}

	var stdout bytes.Buffer
	if err := repo.ExecAndWait(ctx,
		git.SubCmd{
			Name:  "rev-parse",
			Flags: []git.Option{git.Flag{Name: "--verify"}},
			Args:  []string{revision.String()},
		},
		git.WithStderr(ioutil.Discard),
		git.WithStdout(&stdout),
	); err != nil {
		if _, ok := command.ExitStatus(err); ok {
			return "", git.ErrReferenceNotFound
		}
		return "", err
	}

	hex := strings.TrimSpace(stdout.String())
	oid, err := git.NewObjectIDFromHex(hex)
	if err != nil {
		return "", fmt.Errorf("unsupported object hash %q: %w", hex, err)
	}

	return oid, nil
}

// GetReference looks up and returns the given reference. Returns a
// ReferenceNotFound error if the reference was not found.
func (repo *Repo) GetReference(ctx context.Context, reference git.ReferenceName) (git.Reference, error) {
	refs, err := repo.getReferences(ctx, reference.String(), 1)
	if err != nil {
		return git.Reference{}, err
	}

	if len(refs) == 0 {
		return git.Reference{}, git.ErrReferenceNotFound
	}
	if refs[0].Name != reference {
		return git.Reference{}, git.ErrReferenceAmbiguous
	}

	return refs[0], nil
}

// HasBranches determines whether there is at least one branch in the
// repository.
func (repo *Repo) HasBranches(ctx context.Context) (bool, error) {
	refs, err := repo.getReferences(ctx, "refs/heads/", 1)
	return len(refs) > 0, err
}

// GetReferences returns references matching the given pattern.
func (repo *Repo) GetReferences(ctx context.Context, pattern string) ([]git.Reference, error) {
	return repo.getReferences(ctx, pattern, 0)
}

func (repo *Repo) getReferences(ctx context.Context, pattern string, limit uint) ([]git.Reference, error) {
	flags := []git.Option{git.Flag{Name: "--format=%(refname)%00%(objectname)%00%(symref)"}}
	if limit > 0 {
		flags = append(flags, git.Flag{Name: fmt.Sprintf("--count=%d", limit)})
	}

	var args []string
	if pattern != "" {
		args = []string{pattern}
	}

	cmd, err := repo.Exec(ctx, git.SubCmd{
		Name:  "for-each-ref",
		Flags: flags,
		Args:  args,
	})
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(cmd)

	var refs []git.Reference
	for scanner.Scan() {
		line := bytes.SplitN(scanner.Bytes(), []byte{0}, 3)
		if len(line) != 3 {
			return nil, errors.New("unexpected reference format")
		}

		if len(line[2]) == 0 {
			refs = append(refs, git.NewReference(git.ReferenceName(line[0]), string(line[1])))
		} else {
			refs = append(refs, git.NewSymbolicReference(git.ReferenceName(line[0]), string(line[1])))
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
func (repo *Repo) GetBranches(ctx context.Context) ([]git.Reference, error) {
	return repo.GetReferences(ctx, "refs/heads/")
}

// UpdateRef updates reference from oldValue to newValue. If oldValue is a
// non-empty string, the update will fail it the reference is not currently at
// that revision. If newValue is the ZeroOID, the reference will be deleted.
// If oldValue is the ZeroOID, the reference will created.
func (repo *Repo) UpdateRef(ctx context.Context, reference git.ReferenceName, newValue, oldValue git.ObjectID) error {
	var stderr bytes.Buffer

	if err := repo.ExecAndWait(ctx,
		git.SubCmd{
			Name:  "update-ref",
			Flags: []git.Option{git.Flag{Name: "-z"}, git.Flag{Name: "--stdin"}},
		},
		git.WithStdin(strings.NewReader(fmt.Sprintf("update %s\x00%s\x00%s\x00", reference, newValue.String(), oldValue.String()))),
		git.WithStderr(&stderr),
		git.WithRefTxHook(ctx, repo, repo.cfg),
	); err != nil {
		return fmt.Errorf("UpdateRef: failed updating reference %q from %q to %q: %w", reference, oldValue, newValue, errorWithStderr(err, stderr.Bytes()))
	}

	return nil
}
