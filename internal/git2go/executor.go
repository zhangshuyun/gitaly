package git2go

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// Executor executes gitaly-git2go.
type Executor struct {
	binaryPath    string
	gitBinaryPath string
}

// NewExecutor returns a new gitaly-git2go executor using binaries as configured in the given
// configuration.
func NewExecutor(cfg config.Cfg) Executor {
	return Executor{
		binaryPath:    BinaryPath(cfg.BinDir),
		gitBinaryPath: cfg.Git.BinPath,
	}
}
