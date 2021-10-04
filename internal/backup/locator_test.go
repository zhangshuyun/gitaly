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

		expected := &Step{
			SkippableOnNotFound: true,
			BundlePath:          repo.RelativePath + ".bundle",
			RefPath:             repo.RelativePath + ".refs",
			CustomHooksPath:     filepath.Join(repo.RelativePath, "custom_hooks.tar"),
		}

		full := l.BeginFull(ctx, repo, "abc123")
		assert.Equal(t, expected, full)

		require.NoError(t, l.CommitFull(ctx, full))
	})

	t.Run("FindLatest", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		expected := &Backup{
			Steps: []Step{
				{
					SkippableOnNotFound: true,
					BundlePath:          repo.RelativePath + ".bundle",
					RefPath:             repo.RelativePath + ".refs",
					CustomHooksPath:     filepath.Join(repo.RelativePath, "custom_hooks.tar"),
				},
			},
		}

		full, err := l.FindLatest(ctx, repo)
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

		const expectedIncrement = "001"
		expected := &Step{
			BundlePath:      filepath.Join(repo.RelativePath, backupID, expectedIncrement+".bundle"),
			RefPath:         filepath.Join(repo.RelativePath, backupID, expectedIncrement+".refs"),
			CustomHooksPath: filepath.Join(repo.RelativePath, backupID, expectedIncrement+".custom_hooks.tar"),
		}

		full := l.BeginFull(ctx, repo, backupID)
		assert.Equal(t, expected, full)

		require.NoError(t, l.CommitFull(ctx, full))

		backupPointer := testhelper.MustReadFile(t, filepath.Join(backupPath, repo.RelativePath, "LATEST"))
		require.Equal(t, backupID, string(backupPointer))

		incrementPointer := testhelper.MustReadFile(t, filepath.Join(backupPath, repo.RelativePath, backupID, "LATEST"))
		require.Equal(t, expectedIncrement, string(incrementPointer))
	})

	t.Run("FindLatest", func(t *testing.T) {
		t.Run("no fallback", func(t *testing.T) {
			backupPath := testhelper.TempDir(t)
			var l Locator = PointerLocator{
				Sink: NewFilesystemSink(backupPath),
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := l.FindLatest(ctx, repo)
			require.ErrorIs(t, err, ErrDoesntExist)

			require.NoError(t, os.MkdirAll(filepath.Join(backupPath, repo.RelativePath, backupID), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(backupPath, repo.RelativePath, "LATEST"), []byte(backupID), 0o644))
			require.NoError(t, os.WriteFile(filepath.Join(backupPath, repo.RelativePath, backupID, "LATEST"), []byte("003"), 0o644))
			expected := &Backup{
				Steps: []Step{
					{
						BundlePath:      filepath.Join(repo.RelativePath, backupID, "001.bundle"),
						RefPath:         filepath.Join(repo.RelativePath, backupID, "001.refs"),
						CustomHooksPath: filepath.Join(repo.RelativePath, backupID, "001.custom_hooks.tar"),
					},
					{
						BundlePath:      filepath.Join(repo.RelativePath, backupID, "002.bundle"),
						RefPath:         filepath.Join(repo.RelativePath, backupID, "002.refs"),
						PreviousRefPath: filepath.Join(repo.RelativePath, backupID, "001.refs"),
						CustomHooksPath: filepath.Join(repo.RelativePath, backupID, "002.custom_hooks.tar"),
					},
					{
						BundlePath:      filepath.Join(repo.RelativePath, backupID, "003.bundle"),
						RefPath:         filepath.Join(repo.RelativePath, backupID, "003.refs"),
						PreviousRefPath: filepath.Join(repo.RelativePath, backupID, "002.refs"),
						CustomHooksPath: filepath.Join(repo.RelativePath, backupID, "003.custom_hooks.tar"),
					},
				},
			}

			full, err := l.FindLatest(ctx, repo)
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

			expectedFallback := &Backup{
				Steps: []Step{
					{
						SkippableOnNotFound: true,
						BundlePath:          repo.RelativePath + ".bundle",
						RefPath:             repo.RelativePath + ".refs",
						CustomHooksPath:     filepath.Join(repo.RelativePath, "custom_hooks.tar"),
					},
				},
			}

			fallbackFull, err := l.FindLatest(ctx, repo)
			require.NoError(t, err)
			require.Equal(t, expectedFallback, fallbackFull)

			require.NoError(t, os.MkdirAll(filepath.Join(backupPath, repo.RelativePath, backupID), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(backupPath, repo.RelativePath, "LATEST"), []byte(backupID), 0o644))
			require.NoError(t, os.WriteFile(filepath.Join(backupPath, repo.RelativePath, backupID, "LATEST"), []byte("001"), 0o644))
			expected := &Backup{
				Steps: []Step{
					{
						BundlePath:      filepath.Join(repo.RelativePath, backupID, "001.bundle"),
						RefPath:         filepath.Join(repo.RelativePath, backupID, "001.refs"),
						CustomHooksPath: filepath.Join(repo.RelativePath, backupID, "001.custom_hooks.tar"),
					},
				},
			}

			full, err := l.FindLatest(ctx, repo)
			require.NoError(t, err)
			require.Equal(t, expected, full)
		})

		t.Run("invalid backup LATEST", func(t *testing.T) {
			backupPath := testhelper.TempDir(t)
			var l Locator = PointerLocator{
				Sink: NewFilesystemSink(backupPath),
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := l.FindLatest(ctx, repo)
			require.ErrorIs(t, err, ErrDoesntExist)

			require.NoError(t, os.MkdirAll(filepath.Join(backupPath, repo.RelativePath), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(backupPath, repo.RelativePath, "LATEST"), []byte("invalid"), 0o644))
			_, err = l.FindLatest(ctx, repo)
			require.EqualError(t, err, "pointer locator: latest incremental: find latest ID: filesystem sink: get reader for \"TestPointerLocator/invalid/LATEST\": doesn't exist")
		})

		t.Run("invalid incremental LATEST", func(t *testing.T) {
			backupPath := testhelper.TempDir(t)
			var l Locator = PointerLocator{
				Sink: NewFilesystemSink(backupPath),
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := l.FindLatest(ctx, repo)
			require.ErrorIs(t, err, ErrDoesntExist)

			require.NoError(t, os.MkdirAll(filepath.Join(backupPath, repo.RelativePath, backupID), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(backupPath, repo.RelativePath, "LATEST"), []byte(backupID), 0o644))
			require.NoError(t, os.WriteFile(filepath.Join(backupPath, repo.RelativePath, backupID, "LATEST"), []byte("invalid"), 0o644))

			_, err = l.FindLatest(ctx, repo)
			require.EqualError(t, err, "pointer locator: latest incremental: strconv.Atoi: parsing \"invalid\": invalid syntax")
		})
	})
}
