package hooks

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

func TestPath(t *testing.T) {
	defer func(rubyDir, shellDir string) {
		config.Config.Ruby.Dir = rubyDir
		config.Config.GitlabShell.Dir = shellDir
	}(config.Config.Ruby.Dir, config.Config.GitlabShell.Dir)
	config.Config.Ruby.Dir = "/bazqux/gitaly-ruby"
	config.Config.GitlabShell.Dir = "/foobar/gitlab-shell"

	hooksVar := "GITALY_USE_EMBEDDED_HOOKS"
	t.Run(fmt.Sprintf("with %s=1", hooksVar), func(t *testing.T) {
		os.Setenv(hooksVar, "1")
		defer os.Unsetenv(hooksVar)

		require.Equal(t, "/bazqux/gitaly-ruby/git-hooks", Path())
	})

	t.Run(fmt.Sprintf("without %s=1", hooksVar), func(t *testing.T) {
		os.Unsetenv(hooksVar)

		require.Equal(t, "/foobar/gitlab-shell/hooks", Path())
	})

	t.Run("with an override", func(t *testing.T) {
		Override = "/override/hooks"
		defer func() { Override = "" }()

		require.Equal(t, "/override/hooks", Path())
	})
}
