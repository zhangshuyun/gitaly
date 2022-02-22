package git

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// ErrNotConfigured may be returned by an ExecutionEnvironmentConstructor in case an environment
// was not configured.
var ErrNotConfigured = errors.New("execution environment is not configured")

// ExecutionEnvironment describes the environment required to execute a Git command
type ExecutionEnvironment struct {
	// BinaryPath is the path to the Git binary.
	BinaryPath string
	// EnvironmentVariables are variables which must be set when running the Git binary.
	EnvironmentVariables []string
}

// DistributedGitEnvironmentConstructor creates ExecutionEnvironments via the Git binary path
// configured in the Gitaly configuration. This expects a complete Git installation with all its
// components. The installed distribution must either have its prefix compiled into the binaries or
// alternatively be compiled with runtime-detection of the prefix such that Git is able to locate
// its auxiliary helper binaries correctly.
type DistributedGitEnvironmentConstructor struct{}

// Construct sets up an ExecutionEnvironment for a complete Git distribution. No setup needs to be
// performed given that the Git environment is expected to be self-contained. The returned function
// is a cleanup function that shall be executed when the ExecutionEnvironment is no longer used.
//
// For testing purposes, this function overrides the configured Git binary path if the
// `GITALY_TESTING_GIT_BINARY` environment variable is set.
func (c DistributedGitEnvironmentConstructor) Construct(cfg config.Cfg) (ExecutionEnvironment, func(), error) {
	binaryPath := cfg.Git.BinPath
	if override := os.Getenv("GITALY_TESTING_GIT_BINARY"); binaryPath == "" && override != "" {
		binaryPath = override
	}

	if binaryPath == "" {
		return ExecutionEnvironment{}, nil, ErrNotConfigured
	}

	return ExecutionEnvironment{
		BinaryPath: binaryPath,
	}, func() {}, nil
}

// BundledGitEnvironmentConstructor sets up an ExecutionEnvironment for a bundled Git installation.
// Bundled Git is a partial Git installation, where only a subset of Git binaries are installed
// into Gitaly's binary directory. The binaries must have a `gitaly-` prefix like e.g. `gitaly-git`.
// Bundled Git installations can be installed with Gitaly's Makefile via `make install
// WITH_BUNDLED_GIT=YesPlease`.
type BundledGitEnvironmentConstructor struct{}

// Construct sets up an ExecutionEnvironment for a bundled Git installation. Because bundled Git
// installations are not complete Git installations we need to set up a usable environment at
// runtime. This is done by creating a temporary directory into which we symlink the bundled
// binaries with their usual names as expected by Git. Furthermore, we configure the GIT_EXEC_PATH
// environment variable to point to that directory such that Git is able to locate its auxiliary
// binaries.
//
// For testing purposes, this function will automatically enable use of bundled Git in case the
// `GITALY_TESTING_BUNDLED_GIT_PATH` environment variable is set.
func (c BundledGitEnvironmentConstructor) Construct(cfg config.Cfg) (_ ExecutionEnvironment, _ func(), returnedErr error) {
	useBundledBinaries := cfg.Git.UseBundledBinaries

	if bundledGitPath := os.Getenv("GITALY_TESTING_BUNDLED_GIT_PATH"); bundledGitPath != "" {
		if cfg.BinDir == "" {
			return ExecutionEnvironment{}, nil, errors.New("cannot use bundled binaries without bin path being set")
		}

		// We need to symlink pre-built Git binaries into Gitaly's binary directory.
		// Normally they would of course already exist there, but in tests we create a new
		// binary directory for each server and thus need to populate it first.
		for _, binary := range []string{"gitaly-git", "gitaly-git-remote-http", "gitaly-git-http-backend"} {
			bundledGitBinary := filepath.Join(bundledGitPath, binary)
			if _, err := os.Stat(bundledGitBinary); err != nil {
				return ExecutionEnvironment{}, nil, fmt.Errorf("statting %q: %w", binary, err)
			}

			if err := os.Symlink(bundledGitBinary, filepath.Join(cfg.BinDir, binary)); err != nil {
				// While Gitaly's Go tests use a temporary binary directory, Ruby
				// rspecs set up the binary directory to point to our build
				// directory. They thus already contain the Git binaries and don't
				// need symlinking.
				if errors.Is(err, os.ErrExist) {
					continue
				}
				return ExecutionEnvironment{}, nil, fmt.Errorf("symlinking bundled %q: %w", binary, err)
			}
		}

		useBundledBinaries = true
	}

	if !useBundledBinaries {
		return ExecutionEnvironment{}, nil, ErrNotConfigured
	}

	// In order to support having a single Git binary only as compared to a complete Git
	// installation, we create our own GIT_EXEC_PATH which contains symlinks to the Git
	// binary for executables which Git expects to be present.
	gitExecPath, err := os.MkdirTemp("", "gitaly-git-exec-path-*")
	if err != nil {
		return ExecutionEnvironment{}, nil, fmt.Errorf("creating Git exec path: %w", err)
	}

	cleanup := func() {
		if err := os.RemoveAll(gitExecPath); err != nil {
			logrus.WithError(err).Error("cleanup of Git execution environment failed")
		}
	}
	defer func() {
		if returnedErr != nil {
			cleanup()
		}
	}()

	for executable, target := range map[string]string{
		"git":                "gitaly-git",
		"git-receive-pack":   "gitaly-git",
		"git-upload-pack":    "gitaly-git",
		"git-upload-archive": "gitaly-git",
		"git-http-backend":   "gitaly-git-http-backend",
		"git-remote-http":    "gitaly-git-remote-http",
		"git-remote-https":   "gitaly-git-remote-http",
		"git-remote-ftp":     "gitaly-git-remote-http",
		"git-remote-ftps":    "gitaly-git-remote-http",
	} {
		if err := os.Symlink(
			filepath.Join(cfg.BinDir, target),
			filepath.Join(gitExecPath, executable),
		); err != nil {
			return ExecutionEnvironment{}, nil, fmt.Errorf("linking Git executable %q: %w", executable, err)
		}
	}

	return ExecutionEnvironment{
		BinaryPath: filepath.Join(gitExecPath, "git"),
		EnvironmentVariables: []string{
			"GIT_EXEC_PATH=" + gitExecPath,
		},
	}, cleanup, nil
}

// FallbackGitEnvironmentConstructor sets up a fallback execution environment where Git is resolved
// via the `PATH` environment variable. This is only intended as a last resort in case no other
// environments have been set up.
type FallbackGitEnvironmentConstructor struct{}

// Construct sets up an execution environment by searching `PATH` for a `git` executable.
func (c FallbackGitEnvironmentConstructor) Construct(config.Cfg) (ExecutionEnvironment, func(), error) {
	resolvedPath, err := exec.LookPath("git")
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return ExecutionEnvironment{}, nil, fmt.Errorf("%w: no git executable found in PATH", ErrNotConfigured)
		}

		return ExecutionEnvironment{}, nil, fmt.Errorf("resolving git executable: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"resolvedPath": resolvedPath,
	}).Warn("git path not configured. Using default path resolution")

	return ExecutionEnvironment{
		BinaryPath: resolvedPath,
	}, func() {}, nil
}
