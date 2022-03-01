package git

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"golang.org/x/sys/unix"
)

var (
	// ErrNotConfigured may be returned by an ExecutionEnvironmentConstructor in case an
	// environment was not configured.
	ErrNotConfigured = errors.New("execution environment is not configured")

	// ExecutionEnvironmentConstructors is the list of Git environments supported by the Git
	// command factory. The order is important and signifies the priority in which the
	// environments will be used: the environment created by the first constructor is the one
	// that will be preferred when executing Git commands. Later environments may be used in
	// case `IsEnabled()` returns `false` though.
	ExecutionEnvironmentConstructors = []ExecutionEnvironmentConstructor{
		BundledGitEnvironmentConstructor{
			// This is the current default bundled Git environment, which does not yet
			// have a version suffix.
			Suffix: "",
			FeatureFlags: []featureflag.FeatureFlag{
				featureflag.UseBundledGit,
			},
		},
		DistributedGitEnvironmentConstructor{},
		FallbackGitEnvironmentConstructor{},
	}
)

// ExecutionEnvironmentConstructor is an interface for constructors of Git execution environments.
// A constructor should be able to set up an environment in which it is possible to run Git
// executables.
type ExecutionEnvironmentConstructor interface {
	Construct(config.Cfg) (ExecutionEnvironment, error)
}

// ExecutionEnvironment describes the environment required to execute a Git command
type ExecutionEnvironment struct {
	// BinaryPath is the path to the Git binary.
	BinaryPath string
	// EnvironmentVariables are variables which must be set when running the Git binary.
	EnvironmentVariables []string

	isEnabled func(context.Context) bool
	cleanup   func()
}

// Cleanup cleans up any state set up by this ExecutionEnvironment.
func (e ExecutionEnvironment) Cleanup() {
	if e.cleanup != nil {
		e.cleanup()
	}
}

// IsEnabled checks whether the ExecutionEnvironment is enabled in the given context. An execution
// environment will typically be enabled by default, except if it's feature-flagged.
func (e ExecutionEnvironment) IsEnabled(ctx context.Context) bool {
	if e.isEnabled != nil {
		return e.isEnabled(ctx)
	}

	return true
}

// DistributedGitEnvironmentConstructor creates ExecutionEnvironments via the Git binary path
// configured in the Gitaly configuration. This expects a complete Git installation with all its
// components. The installed distribution must either have its prefix compiled into the binaries or
// alternatively be compiled with runtime-detection of the prefix such that Git is able to locate
// its auxiliary helper binaries correctly.
type DistributedGitEnvironmentConstructor struct{}

// Construct sets up an ExecutionEnvironment for a complete Git distribution. No setup needs to be
// performed given that the Git environment is expected to be self-contained.
//
// For testing purposes, this function overrides the configured Git binary path if the
// `GITALY_TESTING_GIT_BINARY` environment variable is set.
func (c DistributedGitEnvironmentConstructor) Construct(cfg config.Cfg) (ExecutionEnvironment, error) {
	binaryPath := cfg.Git.BinPath
	if override := os.Getenv("GITALY_TESTING_GIT_BINARY"); binaryPath == "" && override != "" {
		binaryPath = override
	}

	if binaryPath == "" {
		return ExecutionEnvironment{}, ErrNotConfigured
	}

	return ExecutionEnvironment{
		BinaryPath: binaryPath,
	}, nil
}

// BundledGitEnvironmentConstructor sets up an ExecutionEnvironment for a bundled Git installation.
// Bundled Git is a partial Git installation, where only a subset of Git binaries are installed
// into Gitaly's binary directory. The binaries must have a `gitaly-` prefix like e.g. `gitaly-git`.
// Bundled Git installations can be installed with Gitaly's Makefile via `make install
// WITH_BUNDLED_GIT=YesPlease`.
type BundledGitEnvironmentConstructor struct {
	// Suffix is the version suffix used for this specific bundled Git environment. In case
	// multiple sets of bundled Git versions are installed it is possible to also have multiple
	// of these bundled Git environments with different suffixes.
	Suffix string
	// FeatureFlags is the set of feature flags which must be enabled in order for the bundled
	// Git environment to be enabled. Note that _all_ feature flags must be set to `true` in the
	// context.
	FeatureFlags []featureflag.FeatureFlag
}

// Construct sets up an ExecutionEnvironment for a bundled Git installation. Because bundled Git
// installations are not complete Git installations we need to set up a usable environment at
// runtime. This is done by creating a temporary directory into which we symlink the bundled
// binaries with their usual names as expected by Git. Furthermore, we configure the GIT_EXEC_PATH
// environment variable to point to that directory such that Git is able to locate its auxiliary
// binaries.
//
// For testing purposes, this function will automatically enable use of bundled Git in case the
// `GITALY_TESTING_BUNDLED_GIT_PATH` environment variable is set.
func (c BundledGitEnvironmentConstructor) Construct(cfg config.Cfg) (_ ExecutionEnvironment, returnedErr error) {
	useBundledBinaries := cfg.Git.UseBundledBinaries

	if bundledGitPath := os.Getenv("GITALY_TESTING_BUNDLED_GIT_PATH"); bundledGitPath != "" {
		if cfg.BinDir == "" {
			return ExecutionEnvironment{}, errors.New("cannot use bundled binaries without bin path being set")
		}

		// We need to symlink pre-built Git binaries into Gitaly's binary directory.
		// Normally they would of course already exist there, but in tests we create a new
		// binary directory for each server and thus need to populate it first.
		for _, binary := range []string{"gitaly-git", "gitaly-git-remote-http", "gitaly-git-http-backend"} {
			binary := binary + c.Suffix

			bundledGitBinary := filepath.Join(bundledGitPath, binary)
			if _, err := os.Stat(bundledGitBinary); err != nil {
				return ExecutionEnvironment{}, fmt.Errorf("statting %q: %w", binary, err)
			}

			if err := os.Symlink(bundledGitBinary, filepath.Join(cfg.BinDir, binary)); err != nil {
				// While Gitaly's Go tests use a temporary binary directory, Ruby
				// rspecs set up the binary directory to point to our build
				// directory. They thus already contain the Git binaries and don't
				// need symlinking.
				if errors.Is(err, os.ErrExist) {
					continue
				}
				return ExecutionEnvironment{}, fmt.Errorf("symlinking bundled %q: %w", binary, err)
			}
		}

		useBundledBinaries = true
	}

	if !useBundledBinaries {
		return ExecutionEnvironment{}, ErrNotConfigured
	}

	if cfg.BinDir == "" {
		return ExecutionEnvironment{}, errors.New("cannot use bundled binaries without bin path being set")
	}

	// In order to support having a single Git binary only as compared to a complete Git
	// installation, we create our own GIT_EXEC_PATH which contains symlinks to the Git
	// binary for executables which Git expects to be present.
	gitExecPath, err := os.MkdirTemp("", "gitaly-git-exec-path-*")
	if err != nil {
		return ExecutionEnvironment{}, fmt.Errorf("creating Git exec path: %w", err)
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
		target := target + c.Suffix
		targetPath := filepath.Join(cfg.BinDir, target)

		if err := unix.Access(targetPath, unix.X_OK); err != nil {
			return ExecutionEnvironment{}, fmt.Errorf("checking bundled Git binary %q: %w", target, err)
		}

		if err := os.Symlink(
			targetPath,
			filepath.Join(gitExecPath, executable),
		); err != nil {
			return ExecutionEnvironment{}, fmt.Errorf("linking Git executable %q: %w", executable, err)
		}
	}

	return ExecutionEnvironment{
		BinaryPath: filepath.Join(gitExecPath, "git"),
		EnvironmentVariables: []string{
			"GIT_EXEC_PATH=" + gitExecPath,
		},
		cleanup: cleanup,
		isEnabled: func(ctx context.Context) bool {
			for _, flag := range c.FeatureFlags {
				if flag.IsDisabled(ctx) {
					return false
				}
			}

			return true
		},
	}, nil
}

// FallbackGitEnvironmentConstructor sets up a fallback execution environment where Git is resolved
// via the `PATH` environment variable. This is only intended as a last resort in case no other
// environments have been set up.
type FallbackGitEnvironmentConstructor struct{}

// Construct sets up an execution environment by searching `PATH` for a `git` executable.
func (c FallbackGitEnvironmentConstructor) Construct(config.Cfg) (ExecutionEnvironment, error) {
	resolvedPath, err := exec.LookPath("git")
	if err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return ExecutionEnvironment{}, fmt.Errorf("%w: no git executable found in PATH", ErrNotConfigured)
		}

		return ExecutionEnvironment{}, fmt.Errorf("resolving git executable: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"resolvedPath": resolvedPath,
	}).Warn("git path not configured. Using default path resolution")

	return ExecutionEnvironment{
		BinaryPath: resolvedPath,
		// We always pretend that this environment is disabled. This has the effect that
		// even if all the other existing execution environments are disabled via feature
		// flags, we will still not use the fallback Git environment but will instead use
		// the feature flagged environments. The fallback will then only be used in the
		// case it is the only existing environment with no other better alternatives.
		isEnabled: func(context.Context) bool {
			return false
		},
	}, nil
}
