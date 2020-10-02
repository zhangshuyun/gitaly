package hooks

import (
	"fmt"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
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

	if config.SkipHooks() {
		return "/var/empty"
	}

	return filepath.Join(config.Config.Ruby.Dir, "git-hooks")
}

// GitPushOptions turns a slice of git push option values into a GIT_PUSH_OPTION_COUNT and individual
// GIT_PUSH_OPTION_0, GIT_PUSH_OPTION_1 etc.
func GitPushOptions(options []string) []string {
	if len(options) == 0 {
		return []string{}
	}

	envVars := []string{fmt.Sprintf("GIT_PUSH_OPTION_COUNT=%d", len(options))}

	for i, pushOption := range options {
		envVars = append(envVars, fmt.Sprintf("GIT_PUSH_OPTION_%d=%s", i, pushOption))
	}

	return envVars
}
