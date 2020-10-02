package git

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v13/internal/command"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/repository"
)

var (
	ErrReferenceNotFound = errors.New("reference not found")
)

// Repository represents a Git repository.
type Repository interface {
	// ResolveRef resolves the given refish to its object ID. This uses the
	// typical DWIM mechanism of Git to resolve the reference. See
	// gitrevisions(1) for accepted syntax. This will not verify whether the
	// object ID exists. To do so, you can peel the reference to a given
	// object type, e.g. by passing `refs/heads/master^{commit}`.
	ResolveRefish(ctx context.Context, ref string) (string, error)

	// ContainsRef checks if a ref in the repository exists. This will not
	// verify whether the target object exists. To do so, you can peel the
	// reference to a given object type, e.g. by passing
	// `refs/heads/master^{commit}`.
	ContainsRef(ctx context.Context, ref string) (bool, error)

	// GetReference looks up and returns the given reference. Returns a
	// ReferenceNotFound error if the reference was not found.
	GetReference(ctx context.Context, ref string) (Reference, error)

	// GetReferences returns references matching the given pattern.
	GetReferences(ctx context.Context, pattern string) ([]Reference, error)

	// GetBranch looks up and returns the given branch. Returns a
	// ErrReferenceNotFound if it wasn't found.
	GetBranch(ctx context.Context, branch string) (Reference, error)

	// GetBranches returns all branches.
	GetBranches(ctx context.Context) ([]Reference, error)

	// UpdateRef updates reference from oldrev to newrev. If oldrev is a
	// non-empty string, the update will fail it the reference is not
	// currently at that revision. If newrev is the zero OID, the reference
	// will be deleted. If oldrev is the zero OID, the reference will
	// created.
	UpdateRef(ctx context.Context, reference, newrev, oldrev string) error
}

// localRepository represents a local Git repository.
type localRepository struct {
	repo repository.GitRepo
}

// NewRepository creates a new Repository from its protobuf representation.
func NewRepository(repo repository.GitRepo) Repository {
	return &localRepository{
		repo: repo,
	}
}

// command creates a Git Command with the given args and Repository, executed
// in the Repository. It validates the arguments in the command before
// executing.
func (repo *localRepository) command(ctx context.Context, globals []Option, cmd SubCmd) (*command.Command, error) {
	return SafeStdinCmd(ctx, repo.repo, globals, cmd)
}

func (repo *localRepository) ResolveRefish(ctx context.Context, refish string) (string, error) {
	if refish == "" {
		return "", errors.New("repository cannot contain empty reference name")
	}

	cmd, err := repo.command(ctx, nil, SubCmd{
		Name:  "rev-parse",
		Flags: []Option{Flag{Name: "--verify"}},
		Args:  []string{refish},
	})
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

	oid := strings.TrimSpace(stdout.String())
	if len(oid) != 40 {
		return "", fmt.Errorf("unsupported object hash %q", oid)
	}

	return oid, nil
}

func (repo *localRepository) ContainsRef(ctx context.Context, ref string) (bool, error) {
	if _, err := repo.ResolveRefish(ctx, ref); err != nil {
		if errors.Is(err, ErrReferenceNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (repo *localRepository) GetReference(ctx context.Context, ref string) (Reference, error) {
	refs, err := repo.GetReferences(ctx, ref)
	if err != nil {
		return Reference{}, err
	}

	if len(refs) == 0 {
		return Reference{}, ErrReferenceNotFound
	}

	return refs[0], nil
}

func (repo *localRepository) GetReferences(ctx context.Context, pattern string) ([]Reference, error) {
	var args []string
	if pattern != "" {
		args = []string{pattern}
	}

	cmd, err := repo.command(ctx, nil, SubCmd{
		Name:  "for-each-ref",
		Flags: []Option{Flag{Name: "--format=%(refname)%00%(objectname)%00%(symref)"}},
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
			refs = append(refs, NewReference(string(line[0]), string(line[1])))
		} else {
			refs = append(refs, NewSymbolicReference(string(line[0]), string(line[1])))
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

func (repo *localRepository) GetBranch(ctx context.Context, branch string) (Reference, error) {
	if strings.HasPrefix(branch, "refs/heads/") {
		return repo.GetReference(ctx, branch)
	}

	if strings.HasPrefix(branch, "heads/") {
		branch = strings.TrimPrefix(branch, "heads/")
	}
	return repo.GetReference(ctx, "refs/heads/"+branch)
}

func (repo *localRepository) GetBranches(ctx context.Context) ([]Reference, error) {
	return repo.GetReferences(ctx, "refs/heads/")
}

func (repo *localRepository) UpdateRef(ctx context.Context, reference, newrev, oldrev string) error {
	cmd, err := repo.command(ctx, nil, SubCmd{
		Name:  "update-ref",
		Flags: []Option{Flag{Name: "-z"}, Flag{Name: "--stdin"}},
	})
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(cmd, "update %s\x00%s\x00%s\x00", reference, newrev, oldrev); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("UpdateRef: failed updating reference %q from %q to %q: %v", reference, newrev, oldrev, err)
	}

	return nil
}
