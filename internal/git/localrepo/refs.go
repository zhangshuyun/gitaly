package localrepo

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
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
	refs, err := repo.getReferences(ctx, 1, reference.String())
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
	refs, err := repo.getReferences(ctx, 1, "refs/heads/")
	return len(refs) > 0, err
}

// GetReferences returns references matching any of the given patterns. If no patterns are given,
// all references are returned.
func (repo *Repo) GetReferences(ctx context.Context, patterns ...string) ([]git.Reference, error) {
	return repo.getReferences(ctx, 0, patterns...)
}

func (repo *Repo) getReferences(ctx context.Context, limit uint, patterns ...string) ([]git.Reference, error) {
	flags := []git.Option{git.Flag{Name: "--format=%(refname)%00%(objectname)%00%(symref)"}}
	if limit > 0 {
		flags = append(flags, git.Flag{Name: fmt.Sprintf("--count=%d", limit)})
	}

	cmd, err := repo.Exec(ctx, git.SubCmd{
		Name:  "for-each-ref",
		Flags: flags,
		Args:  patterns,
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

type getRemoteReferenceConfig struct {
	patterns   []string
	config     []git.ConfigPair
	sshCommand string
}

// GetRemoteReferencesOption is an option which can be passed to GetRemoteReferences.
type GetRemoteReferencesOption func(*getRemoteReferenceConfig)

// WithPatterns sets up a set of patterns which are then used to filter the list of returned
// references.
func WithPatterns(patterns ...string) GetRemoteReferencesOption {
	return func(cfg *getRemoteReferenceConfig) {
		cfg.patterns = patterns
	}
}

// WithSSHCommand sets the SSH invocation to use when communicating with the remote.
func WithSSHCommand(cmd string) GetRemoteReferencesOption {
	return func(cfg *getRemoteReferenceConfig) {
		cfg.sshCommand = cmd
	}
}

// WithConfig sets up Git configuration for the git-ls-remote(1) invocation. The config pairs are
// set up via `WithConfigEnv()` and are thus not exposed via the command line.
func WithConfig(config ...git.ConfigPair) GetRemoteReferencesOption {
	return func(cfg *getRemoteReferenceConfig) {
		cfg.config = config
	}
}

// GetRemoteReferences lists references of the remote. Peeled tags are not returned.
func (repo *Repo) GetRemoteReferences(ctx context.Context, remote string, opts ...GetRemoteReferencesOption) ([]git.Reference, error) {
	var cfg getRemoteReferenceConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	var env []string
	if cfg.sshCommand != "" {
		env = append(env, envGitSSHCommand(cfg.sshCommand))
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	if err := repo.ExecAndWait(ctx,
		git.SubCmd{
			Name: "ls-remote",
			Flags: []git.Option{
				git.Flag{Name: "--refs"},
				git.Flag{Name: "--symref"},
			},
			Args: append([]string{remote}, cfg.patterns...),
		},
		git.WithStdout(stdout),
		git.WithStderr(stderr),
		git.WithEnv(env...),
		git.WithConfigEnv(cfg.config...),
	); err != nil {
		return nil, fmt.Errorf("create git ls-remote: %w, stderr: %q", err, stderr)
	}

	var refs []git.Reference
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		split := strings.SplitN(scanner.Text(), "\t", 2)
		if len(split) != 2 {
			return nil, fmt.Errorf("invalid ls-remote output line: %q", scanner.Text())
		}

		// Symbolic references are outputted as:
		//	ref: refs/heads/master	refs/heads/symbolic-ref
		//	0c9cf732b5774fa948348bbd6f273009bd66e04c	refs/heads/symbolic-ref
		if strings.HasPrefix(split[0], "ref: ") {
			symRef := split[1]
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					return nil, fmt.Errorf("scan dereferenced symbolic ref: %w", err)
				}

				return nil, fmt.Errorf("missing dereferenced symbolic ref line for %q", symRef)
			}

			split = strings.SplitN(scanner.Text(), "\t", 2)
			if len(split) != 2 {
				return nil, fmt.Errorf("invalid dereferenced symbolic ref line: %q", scanner.Text())
			}

			if split[1] != symRef {
				return nil, fmt.Errorf("expected dereferenced symbolic ref %q but got reference %q", symRef, split[1])
			}

			refs = append(refs, git.NewSymbolicReference(git.ReferenceName(symRef), split[0]))
			continue
		}

		refs = append(refs, git.NewReference(git.ReferenceName(split[1]), split[0]))
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	return refs, nil
}

// GetDefaultBranch determines the default branch name
func (repo *Repo) GetDefaultBranch(ctx context.Context) (git.ReferenceName, error) {
	branches, err := repo.GetBranches(ctx)
	if err != nil {
		return "", err
	}
	switch len(branches) {
	case 0:
		return "", nil
	case 1:
		return branches[0].Name, nil
	}

	headReference, err := repo.headReference(ctx)
	if err != nil {
		return "", err
	}

	var defaultRef, legacyDefaultRef git.ReferenceName
	for _, branch := range branches {
		if len(headReference) != 0 && headReference == branch.Name {
			return branch.Name, nil
		}

		if string(git.DefaultRef) == branch.Name.String() {
			defaultRef = branch.Name
		}

		if string(git.LegacyDefaultRef) == branch.Name.String() {
			legacyDefaultRef = branch.Name
		}
	}

	if len(defaultRef) != 0 {
		return defaultRef, nil
	}

	if len(legacyDefaultRef) != 0 {
		return legacyDefaultRef, nil
	}

	// If all else fails, return the first branch name
	return branches[0].Name, nil
}

func (repo *Repo) headReference(ctx context.Context) (git.ReferenceName, error) {
	var headRef []byte

	cmd, err := repo.Exec(ctx, git.SubCmd{
		Name:  "rev-parse",
		Flags: []git.Option{git.Flag{Name: "--symbolic-full-name"}},
		Args:  []string{"HEAD"},
	})
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(cmd)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return "", err
	}
	headRef = scanner.Bytes()

	if err := cmd.Wait(); err != nil {
		// If the ref pointed at by HEAD doesn't exist, the rev-parse fails
		// returning the string `"HEAD"`, so we return `nil` without error.
		if bytes.Equal(headRef, []byte("HEAD")) {
			return "", nil
		}

		return "", err
	}

	return git.ReferenceName(headRef), nil
}
