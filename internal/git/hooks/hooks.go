package hooks

import (
	"os"
	"path"

	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// Override allows tests to control where the hooks directory is.
var Override string

// Path returns the path where the global git hooks are located. If the
// environment variable GITALY_TESTING_NO_GIT_HOOKS is set to "1", Path
// will return an empty directory, which has the effect that no Git hooks
// will run at all.
func Path() string {
	if len(Override) > 0 {
		return Override
	}

	if os.Getenv("GITALY_TESTING_NO_GIT_HOOKS") == "1" {
		return "/var/empty"
	}

	return path.Join(config.Config.Ruby.Dir, "git-hooks")
}

// GitPath returns the path where the go implemeneted global git hooks are located
func GitPath() string {
	if len(Override) > 0 {
		return Override
	}

	if os.Getenv("GITALY_TESTING_NO_GIT_HOOKS") == "1" {
		return "/var/empty"
	}

	return path.Join(config.Config.BinDir, "hooks")
}
