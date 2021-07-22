package git2go

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/alternates"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/version"
)

const rawBinaryName = "gitaly-git2go"

var (
	// ErrInvalidArgument is returned in case the merge arguments are invalid.
	ErrInvalidArgument = errors.New("invalid parameters")

	// BinaryName is a binary name with version suffix .
	BinaryName = rawBinaryName + "-" + version.GetModuleVersion()
)

// Executor executes gitaly-git2go.
type Executor struct {
	binaryPath    string
	gitBinaryPath string
	locator       storage.Locator
}

// NewExecutor returns a new gitaly-git2go executor using binaries as configured in the given
// configuration.
func NewExecutor(cfg config.Cfg, locator storage.Locator) Executor {
	return Executor{
		binaryPath:    BinaryPath(cfg.BinDir),
		gitBinaryPath: cfg.Git.BinPath,
		locator:       locator,
	}
}

// BinaryPath returns path to the executable binary.
func BinaryPath(binaryFolder string) string {
	// At first try to find the versioned binary
	path := filepath.Join(binaryFolder, BinaryName)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		// if not exist fall back to the old unversioned binary
		path = filepath.Join(binaryFolder, rawBinaryName)
	}
	return path
}

func (b Executor) run(ctx context.Context, repo repository.GitRepo, stdin io.Reader, args ...string) (*bytes.Buffer, error) {
	repoPath, err := b.locator.GetRepoPath(repo)
	if err != nil {
		return nil, fmt.Errorf("gitaly-git2go: %w", err)
	}

	env := alternates.Env(repoPath, repo.GetGitObjectDirectory(), repo.GetGitAlternateObjectDirectories())

	var stderr, stdout bytes.Buffer
	cmd, err := command.New(ctx, exec.Command(b.binaryPath, args...), stdin, &stdout, &stderr, env...)
	if err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("%s", stderr.String())
		}
		return nil, err
	}

	return &stdout, nil
}

// runWithGob runs the specified gitaly-git2go cmd with the request gob-encoded
// as input and returns the commit ID as string or an error.
func (b Executor) runWithGob(ctx context.Context, repo repository.GitRepo, cmd string, request interface{}) (git.ObjectID, error) {
	input := &bytes.Buffer{}
	if err := gob.NewEncoder(input).Encode(request); err != nil {
		return "", fmt.Errorf("%s: %w", cmd, err)
	}

	output, err := b.run(ctx, repo, input, cmd)
	if err != nil {
		return "", fmt.Errorf("%s: %w", cmd, err)
	}

	var result Result
	if err := gob.NewDecoder(output).Decode(&result); err != nil {
		return "", fmt.Errorf("%s: %w", cmd, err)
	}

	if result.Error != nil {
		return "", fmt.Errorf("%s: %w", cmd, result.Error)
	}

	commitID, err := git.NewObjectIDFromHex(result.CommitID)
	if err != nil {
		return "", fmt.Errorf("could not parse commit ID: %w", err)
	}

	return commitID, nil
}
