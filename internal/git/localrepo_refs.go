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

	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

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

// HasBranches determines whether there is at least one branch in the
// repository.
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
