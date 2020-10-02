package hooks

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
)

func TestPath(t *testing.T) {
	defer func(rubyDir string) {
		config.Config.Ruby.Dir = rubyDir
	}(config.Config.Ruby.Dir)
	config.Config.Ruby.Dir = "/bazqux/gitaly-ruby"

	t.Run("default", func(t *testing.T) {
		require.Equal(t, "/bazqux/gitaly-ruby/git-hooks", Path())
	})

	t.Run("with an override", func(t *testing.T) {
		Override = "/override/hooks"
		defer func() { Override = "" }()

		require.Equal(t, "/override/hooks", Path())
	})

	t.Run("when an env override", func(t *testing.T) {
		key := "GITALY_TESTING_NO_GIT_HOOKS"

		os.Setenv(key, "1")
		defer os.Unsetenv(key)

		require.Equal(t, "/var/empty", Path())
	})
}

func TestGitPushOptions(t *testing.T) {
	testCases := []struct {
		desc     string
		input    []string
		expected []string
	}{
		{
			desc:     "empty input",
			input:    []string{},
			expected: []string{},
		},
		{
			desc:     "nil input",
			input:    nil,
			expected: []string{},
		},
		{
			desc:     "one option",
			input:    []string{"option1"},
			expected: []string{"GIT_PUSH_OPTION_COUNT=1", "GIT_PUSH_OPTION_0=option1"},
		},
		{
			desc:     "multiple options",
			input:    []string{"option1", "option2"},
			expected: []string{"GIT_PUSH_OPTION_COUNT=2", "GIT_PUSH_OPTION_0=option1", "GIT_PUSH_OPTION_1=option2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, GitPushOptions(tc.input))
		})
	}
}
