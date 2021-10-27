package hooks

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestPath(t *testing.T) {
	cfg := config.Cfg{Ruby: config.Ruby{Dir: "/bazqux/gitaly-ruby"}}

	t.Run("default", func(t *testing.T) {
		require.Equal(t, "/bazqux/gitaly-ruby/git-hooks", Path(cfg))
	})

	t.Run("with an override", func(t *testing.T) {
		Override = "/override/hooks"
		defer func() { Override = "" }()

		require.Equal(t, "/override/hooks", Path(cfg))
	})

	t.Run("when an env override", func(t *testing.T) {
		testhelper.ModifyEnvironment(t, "GITALY_TESTING_NO_GIT_HOOKS", "1")

		require.Equal(t, "/var/empty", Path(cfg))
	})
}
