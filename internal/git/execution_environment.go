package git

import (
	"errors"
	"os"

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
