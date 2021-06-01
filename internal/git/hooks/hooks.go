package hooks

import (
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// Override allows tests to control where the hooks directory is.
var Override string

// Path returns the path where the global git hooks are located. If the
// environment variable GITALY_TESTING_NO_GIT_HOOKS is set to "1", Path
// will return an empty directory, which has the effect that no Git hooks
// will run at all.
func Path(cfg config.Cfg) string {
	if len(Override) > 0 {
		return Override
	}

	if config.SkipHooks() {
		return "/var/empty"
	}

	return filepath.Join(cfg.Ruby.Dir, "git-hooks")
}
