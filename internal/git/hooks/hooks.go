package hooks

import (
	"os"
	"path"

	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// Override allows tests to control where the hooks directory is.
var Override string

// Path returns the path where the global git hooks are located. As a
// transitional mechanism, GITALY_USE_EMBEDDED_HOOKS=1 will cause
// Gitaly's embedded hooks to be used instead of the legacy gitlab-shell
// hooks.
func Path() string {
	if len(Override) > 0 {
		return Override
	}

	if os.Getenv("GITALY_USE_EMBEDDED_HOOKS") == "1" {
		return path.Join(config.Config.Ruby.Dir, "git-hooks")
	}

	return path.Join(config.Config.GitlabShell.Dir, "hooks")
}
