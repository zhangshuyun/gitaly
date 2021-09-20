package quarantine

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// entry represents a filesystem entry. An entry represents a dir if children is a non-nil map.
type entry struct {
	children map[string]entry
	contents string
}

func (e entry) create(t *testing.T, root string) {
	// An entry cannot have both file contents and children.
	require.True(t, e.contents == "" || e.children == nil, "An entry cannot have both file contents and children")

	if e.children != nil {
		require.NoError(t, os.Mkdir(root, 0o777))

		for name, child := range e.children {
			child.create(t, filepath.Join(root, name))
		}
	} else {
		require.NoError(t, os.WriteFile(root, []byte(e.contents), 0o666))
	}
}

func TestQuarantine_lifecycle(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	locator := config.NewLocator(cfg)

	t.Run("quarantine directory gets created", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		quarantine, err := New(ctx, repo, locator)
		require.NoError(t, err)

		relativeQuarantinePath, err := filepath.Rel(repoPath, quarantine.dir.Path())
		require.NoError(t, err)

		require.Equal(t, repo, quarantine.repo)
		require.Equal(t, &gitalypb.Repository{
			StorageName:        repo.StorageName,
			RelativePath:       repo.RelativePath,
			GitObjectDirectory: relativeQuarantinePath,
			GitAlternateObjectDirectories: []string{
				"objects",
			},
			GlRepository:  repo.GlRepository,
			GlProjectPath: repo.GlProjectPath,
		}, quarantine.quarantinedRepo)
		require.Equal(t, locator, quarantine.locator)

		require.DirExists(t, quarantine.dir.Path())
	})

	t.Run("context cancellation cleans up quarantine directory", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		quarantine, err := New(ctx, repo, locator)
		require.NoError(t, err)

		require.DirExists(t, quarantine.dir.Path())
		cancel()
		quarantine.dir.WaitForCleanup()
		require.NoDirExists(t, quarantine.dir.Path())
	})
}

func TestQuarantine_Migrate(t *testing.T) {
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)

	t.Run("no changes", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

		oldContents := listEntries(t, repoPath)

		quarantine, err := New(ctx, repo, locator)
		require.NoError(t, err)

		require.NoError(t, quarantine.Migrate())

		require.Equal(t, oldContents, listEntries(t, repoPath))
	})

	t.Run("simple change", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

		oldContents := listEntries(t, repoPath)
		require.NotContains(t, oldContents, "objects/file")

		quarantine, err := New(ctx, repo, locator)
		require.NoError(t, err)

		require.NoError(t, os.WriteFile(filepath.Join(quarantine.dir.Path(), "file"), []byte("foobar"), 0o666))
		require.NoError(t, quarantine.Migrate())

		newContents := listEntries(t, repoPath)
		require.Contains(t, newContents, "objects/file")

		oldContents["objects/file"] = "foobar"
		require.Equal(t, oldContents, newContents)
	})
}

func TestQuarantine_localrepo(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repoProto, _ := testcfg.BuildWithRepo(t)
	locator := config.NewLocator(cfg)

	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	quarantine, err := New(ctx, repoProto, locator)
	require.NoError(t, err)

	quarantined := localrepo.NewTestRepo(t, cfg, quarantine.QuarantinedRepo())

	t.Run("reading unquarantined objects succeeds", func(t *testing.T) {
		_, err := quarantined.ReadObject(ctx, "HEAD^{commit}")
		require.NoError(t, err)
	})

	t.Run("writes are not visible in parent repo", func(t *testing.T) {
		blobID, err := quarantined.WriteBlob(ctx, "", strings.NewReader("contents"))
		require.NoError(t, err)

		_, err = repo.ReadObject(ctx, blobID)
		require.Error(t, err)

		blobContents, err := quarantined.ReadObject(ctx, blobID)
		require.NoError(t, err)
		require.Equal(t, "contents", string(blobContents))
	})

	t.Run("writes are visible after migrating", func(t *testing.T) {
		blobID, err := quarantined.WriteBlob(ctx, "", strings.NewReader("contents"))
		require.NoError(t, err)

		_, err = repo.ReadObject(ctx, blobID)
		require.Error(t, err)

		require.NoError(t, quarantine.Migrate())

		blobContents, err := repo.ReadObject(ctx, blobID)
		require.NoError(t, err)
		require.Equal(t, "contents", string(blobContents))
	})
}

func TestMigrate(t *testing.T) {
	for _, tc := range []struct {
		desc     string
		source   entry
		target   entry
		expected map[string]string
	}{
		{
			desc: "simple migration",
			source: entry{children: map[string]entry{
				"a": {contents: "a"},
				"dir": {children: map[string]entry{
					"b": {contents: "b"},
					"c": {contents: "c"},
				}},
			}},
			target: entry{children: map[string]entry{}},
			expected: map[string]string{
				"a":     "a",
				"dir/":  "",
				"dir/b": "b",
				"dir/c": "c",
			},
		},
		{
			desc: "empty directories",
			source: entry{children: map[string]entry{
				"empty": {children: map[string]entry{
					"dir": {children: map[string]entry{
						"subdir": {children: map[string]entry{}},
					}},
				}},
			}},
			target: entry{children: map[string]entry{}},
			expected: map[string]string{
				"empty/":            "",
				"empty/dir/":        "",
				"empty/dir/subdir/": "",
			},
		},
		{
			desc: "conflicting migration",
			source: entry{children: map[string]entry{
				"does": {children: map[string]entry{
					"not": {children: map[string]entry{
						"exist": {children: map[string]entry{
							"a": {contents: "a"},
						}},
					}},
					"exist": {children: map[string]entry{
						"a": {contents: "a"},
					}},
				}},
			}},
			target: entry{children: map[string]entry{
				"does": {children: map[string]entry{
					"exist": {children: map[string]entry{
						"a": {contents: "conflicting contents"},
					}},
				}},
			}},
			expected: map[string]string{
				"does/":            "",
				"does/not/":        "",
				"does/not/exist/":  "",
				"does/not/exist/a": "a",
				"does/exist/":      "",
				"does/exist/a":     "conflicting contents",
			},
		},
		{
			desc: "dir/file conflict",
			source: entry{children: map[string]entry{
				"conflicting": {children: map[string]entry{
					"path": {children: map[string]entry{}},
				}},
			}},
			target: entry{children: map[string]entry{
				"conflicting": {children: map[string]entry{
					"path": {contents: "imafile"},
				}},
			}},
			expected: map[string]string{
				"conflicting/":     "",
				"conflicting/path": "imafile",
			},
		},
		{
			desc: "file/dir conflict",
			source: entry{children: map[string]entry{
				"conflicting": {children: map[string]entry{
					"path": {contents: "imafile"},
				}},
			}},
			target: entry{children: map[string]entry{
				"conflicting": {children: map[string]entry{
					"path": {children: map[string]entry{}},
				}},
			}},
			expected: map[string]string{
				"conflicting/":      "",
				"conflicting/path/": "",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			dir := testhelper.TempDir(t)

			source := filepath.Join(dir, "source")
			tc.source.create(t, source)

			target := filepath.Join(dir, "target")
			tc.target.create(t, target)

			require.NoError(t, migrate(source, target))
			require.Equal(t, tc.expected, listEntries(t, target))
			require.NoDirExists(t, source)
		})
	}
}

func listEntries(t *testing.T, root string) map[string]string {
	actual := make(map[string]string)

	require.NoError(t, filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		require.NoError(t, err)

		if root == path {
			return nil
		}

		relativePath, err := filepath.Rel(root, path)
		require.NoError(t, err)

		if info.IsDir() {
			actual[relativePath+"/"] = ""
		} else {
			contents := testhelper.MustReadFile(t, path)
			actual[relativePath] = string(contents)
		}

		return nil
	}))

	return actual
}

func TestFinalizeObjectFile(t *testing.T) {
	t.Run("simple migration", func(t *testing.T) {
		dir := testhelper.TempDir(t)

		source := filepath.Join(dir, "a")
		target := filepath.Join(dir, "b")
		require.NoError(t, os.WriteFile(source, []byte("a"), 0o777))

		require.NoError(t, finalizeObjectFile(source, target))
		require.NoFileExists(t, source)
		require.Equal(t, []byte("a"), testhelper.MustReadFile(t, target))
	})

	t.Run("cross-directory migration", func(t *testing.T) {
		sourceDir := testhelper.TempDir(t)
		targetDir := testhelper.TempDir(t)

		source := filepath.Join(sourceDir, "a")
		target := filepath.Join(targetDir, "a")
		require.NoError(t, os.WriteFile(source, []byte("a"), 0o777))

		require.NoError(t, finalizeObjectFile(source, target))
		require.NoFileExists(t, source)
		require.Equal(t, []byte("a"), testhelper.MustReadFile(t, target))
	})

	t.Run("migration with conflict", func(t *testing.T) {
		dir := testhelper.TempDir(t)

		source := filepath.Join(dir, "a")
		require.NoError(t, os.WriteFile(source, []byte("a"), 0o777))

		target := filepath.Join(dir, "b")
		require.NoError(t, os.WriteFile(target, []byte("b"), 0o777))

		// We do not expect an error in case the target file exists: given that objects and
		// packs are content addressable, a file with the same name should have the same
		// contents.
		require.NoError(t, finalizeObjectFile(source, target))
		require.NoFileExists(t, source)
		require.Equal(t, []byte("b"), testhelper.MustReadFile(t, target))
	})

	t.Run("migration with missing source", func(t *testing.T) {
		dir := testhelper.TempDir(t)

		source := filepath.Join(dir, "a")
		target := filepath.Join(dir, "b")

		err := finalizeObjectFile(source, target)
		require.ErrorIs(t, err, os.ErrNotExist)
		require.NoFileExists(t, source)
		require.NoFileExists(t, target)
	})
}

type mockFileInfo struct {
	os.FileInfo
	name string
}

func (e mockFileInfo) Name() string {
	return e.name
}

func TestSortEntries(t *testing.T) {
	for _, tc := range []struct {
		desc     string
		entries  []string
		expected []string
	}{
		{
			desc: "empty",
		},
		{
			desc: "single entry",
			entries: []string{
				"foo",
			},
			expected: []string{
				"foo",
			},
		},
		{
			desc: "multiple non-pack entries are stable",
			entries: []string{
				"foo",
				"bar",
				"qux",
			},
			expected: []string{
				"foo",
				"bar",
				"qux",
			},
		},
		{
			desc: "packfile and metadata sorting",
			entries: []string{
				"pack.keep",
				"2a",
				"pack.rev",
				"pack.pack",
				"pack-foo",
				"pack.idx",
			},
			expected: []string{
				"2a",
				"pack.keep",
				"pack.pack",
				"pack.rev",
				"pack.idx",
				"pack-foo",
			},
		},
		{
			desc: "multiple packfiles",
			entries: []string{
				"pack-1.pack",
				"pack-2.keep",
				"pack-1.keep",
				"pack-2.rev",
				"pack-2.pack",
				"pack-2.idx",
				"pack-3.keep",
				"pack-3.rev",
				"pack-1.idx",
				"pack-3.idx",
				"pack-3.pack",
				"pack-1.rev",
			},
			expected: []string{
				// While we sort by suffix, the relative order should stay the same.
				"pack-2.keep",
				"pack-1.keep",
				"pack-3.keep",
				"pack-1.pack",
				"pack-2.pack",
				"pack-3.pack",
				"pack-2.rev",
				"pack-3.rev",
				"pack-1.rev",
				"pack-2.idx",
				"pack-1.idx",
				"pack-3.idx",
			},
		},
		{
			desc: "mixed packfiles and loose objects",
			entries: []string{
				"pack/pack-1b73f6861d229dc95dc223f0f5ea813aee8737ab.pack",
				"07/7923d0295d21536d8be837e8318a1592b040fe",
				"info/packs",
				"pack/pack-1b73f6861d229dc95dc223f0f5ea813aee8737ab.idx",
				"32/43774a285780f59f20c42d739600d596d9b1de",
				"pack/pack-4ce00e8f93ce33128d4c9e2b14931c458ded8ec2.pack",
				"pack/pack-4ce00e8f93ce33128d4c9e2b14931c458ded8ec2.idx",
			},
			expected: []string{
				"07/7923d0295d21536d8be837e8318a1592b040fe",
				"info/packs",
				"32/43774a285780f59f20c42d739600d596d9b1de",
				"pack/pack-1b73f6861d229dc95dc223f0f5ea813aee8737ab.pack",
				"pack/pack-4ce00e8f93ce33128d4c9e2b14931c458ded8ec2.pack",
				"pack/pack-1b73f6861d229dc95dc223f0f5ea813aee8737ab.idx",
				"pack/pack-4ce00e8f93ce33128d4c9e2b14931c458ded8ec2.idx",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var actualEntries []os.FileInfo
			for _, entry := range tc.entries {
				actualEntries = append(actualEntries, mockFileInfo{name: entry})
			}

			var expectedEntries []os.FileInfo
			for _, entry := range tc.expected {
				expectedEntries = append(expectedEntries, mockFileInfo{name: entry})
			}

			sortEntries(actualEntries)
			require.Equal(t, expectedEntries, actualEntries)
		})
	}
}
