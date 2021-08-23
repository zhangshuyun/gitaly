package backup

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestLegacyLocator(t *testing.T) {
	_, repo, _ := testcfg.BuildWithRepo(t)
	l := LegacyLocator{}

	t.Run("Begin/Commit Full", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		expected := &Full{
			BundlePath:      repo.RelativePath + ".bundle",
			RefPath:         repo.RelativePath + ".refs",
			CustomHooksPath: filepath.Join(repo.RelativePath, "custom_hooks.tar"),
		}

		full, err := l.BeginFull(ctx, repo, "abc123")
		require.NoError(t, err)

		assert.Equal(t, expected, full)

		require.NoError(t, l.CommitFull(ctx, full))
	})

	t.Run("FindLatestFull", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		expected := &Full{
			BundlePath:      repo.RelativePath + ".bundle",
			RefPath:         repo.RelativePath + ".refs",
			CustomHooksPath: filepath.Join(repo.RelativePath, "custom_hooks.tar"),
		}

		full, err := l.FindLatestFull(ctx, repo)
		require.NoError(t, err)

		assert.Equal(t, expected, full)
	})
}
