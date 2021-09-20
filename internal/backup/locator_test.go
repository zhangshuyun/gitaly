package backup

import (
	"os"
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

		full := l.BeginFull(ctx, repo, "abc123")
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

func TestPointerLocator(t *testing.T) {
	const backupID = "abc123"

	_, repo, _ := testcfg.BuildWithRepo(t)

	t.Run("Begin/Commit Full", func(t *testing.T) {
		backupPath := testhelper.TempDir(t)
		var l Locator = PointerLocator{
			Sink: NewFilesystemSink(backupPath),
		}

		ctx, cancel := testhelper.Context()
		defer cancel()

		expected := &Full{
			BundlePath:      filepath.Join(repo.RelativePath, backupID, "full.bundle"),
			RefPath:         filepath.Join(repo.RelativePath, backupID, "full.refs"),
			CustomHooksPath: filepath.Join(repo.RelativePath, backupID, "custom_hooks.tar"),
		}

		full := l.BeginFull(ctx, repo, backupID)
		assert.Equal(t, expected, full)

		require.NoError(t, l.CommitFull(ctx, full))

		pointer := testhelper.MustReadFile(t, filepath.Join(backupPath, repo.RelativePath, "LATEST"))
		require.Equal(t, backupID, string(pointer))
	})

	t.Run("FindLatestFull", func(t *testing.T) {
		t.Run("no fallback", func(t *testing.T) {
			backupPath := testhelper.TempDir(t)
			var l Locator = PointerLocator{
				Sink: NewFilesystemSink(backupPath),
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := l.FindLatestFull(ctx, repo)
			require.ErrorIs(t, err, ErrDoesntExist)

			require.NoError(t, os.MkdirAll(filepath.Join(backupPath, repo.RelativePath), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(backupPath, repo.RelativePath, "LATEST"), []byte(backupID), 0o644))
			expected := &Full{
				BundlePath:      filepath.Join(repo.RelativePath, backupID, "full.bundle"),
				RefPath:         filepath.Join(repo.RelativePath, backupID, "full.refs"),
				CustomHooksPath: filepath.Join(repo.RelativePath, backupID, "custom_hooks.tar"),
			}

			full, err := l.FindLatestFull(ctx, repo)
			require.NoError(t, err)
			require.Equal(t, expected, full)
		})

		t.Run("fallback", func(t *testing.T) {
			backupPath := testhelper.TempDir(t)
			var l Locator = PointerLocator{
				Sink:     NewFilesystemSink(backupPath),
				Fallback: LegacyLocator{},
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			expectedFallback := &Full{
				BundlePath:      repo.RelativePath + ".bundle",
				RefPath:         repo.RelativePath + ".refs",
				CustomHooksPath: filepath.Join(repo.RelativePath, "custom_hooks.tar"),
			}

			fallbackFull, err := l.FindLatestFull(ctx, repo)
			require.NoError(t, err)
			require.Equal(t, expectedFallback, fallbackFull)

			require.NoError(t, os.MkdirAll(filepath.Join(backupPath, repo.RelativePath), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(backupPath, repo.RelativePath, "LATEST"), []byte(backupID), 0o644))
			expected := &Full{
				BundlePath:      filepath.Join(repo.RelativePath, backupID, "full.bundle"),
				RefPath:         filepath.Join(repo.RelativePath, backupID, "full.refs"),
				CustomHooksPath: filepath.Join(repo.RelativePath, backupID, "custom_hooks.tar"),
			}

			full, err := l.FindLatestFull(ctx, repo)
			require.NoError(t, err)
			require.Equal(t, expected, full)
		})
	})
}
