package hooks

import (
	"os"
	"path"

	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// Path returns the path where the global git hooks are located. As a
// transitional mechanism, GITALY_USE_EMBEDDED_HOOKS=1 will cause
// Gitaly's embedded hooks to be used instead of the legacy gitlab-shell
// hooks.
func Path() string {
	if os.Getenv("GITALY_USE_EMBEDDED_HOOKS") == "1" {
		return path.Join(config.Config.Ruby.Dir, "gitlab-shell/hooks")
	}

	return path.Join(config.Config.GitlabShell.Dir, "hooks")
}
