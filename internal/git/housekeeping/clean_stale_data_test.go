package housekeeping

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"google.golang.org/grpc/peer"
)

type entryFinalState int

const (
	Delete entryFinalState = iota
	Keep
)

type entry interface {
	create(t *testing.T, parent string)
	validate(t *testing.T, parent string)
}

// fileEntry is an entry implementation for a file
type fileEntry struct {
	name       string
	mode       os.FileMode
	age        time.Duration
	finalState entryFinalState
}

func (f *fileEntry) create(t *testing.T, parent string) {
	filename := filepath.Join(parent, f.name)
	ff, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0o700)
	assert.NoError(t, err, "file creation failed: %v", filename)
	err = ff.Close()
	assert.NoError(t, err, "file close failed: %v", filename)

	f.chmod(t, filename)
	f.chtimes(t, filename)
}

func (f *fileEntry) validate(t *testing.T, parent string) {
	filename := filepath.Join(parent, f.name)
	f.checkExistence(t, filename)
}

func (f *fileEntry) chmod(t *testing.T, filename string) {
	err := os.Chmod(filename, f.mode)
	assert.NoError(t, err, "chmod failed")
}

func (f *fileEntry) chtimes(t *testing.T, filename string) {
	filetime := time.Now().Add(-f.age)
	err := os.Chtimes(filename, filetime, filetime)
	assert.NoError(t, err, "chtimes failed")
}

func (f *fileEntry) checkExistence(t *testing.T, filename string) {
	_, err := os.Stat(filename)
	if err == nil && f.finalState == Delete {
		t.Errorf("Expected %v to have been deleted.", filename)
	} else if err != nil && f.finalState == Keep {
		t.Errorf("Expected %v to not have been deleted.", filename)
	}
}

// dirEntry is an entry implementation for a directory. A file with entries
type dirEntry struct {
	fileEntry
	entries []entry
}

func (d *dirEntry) create(t *testing.T, parent string) {
	dirname := filepath.Join(parent, d.name)

	if err := os.Mkdir(dirname, 0o700); err != nil {
		require.True(t, os.IsExist(err), "mkdir failed: %v", dirname)
	}

	for _, e := range d.entries {
		e.create(t, dirname)
	}

	// Apply permissions and times after the children have been created
	d.chmod(t, dirname)
	d.chtimes(t, dirname)
}

func (d *dirEntry) validate(t *testing.T, parent string) {
	dirname := filepath.Join(parent, d.name)
	d.checkExistence(t, dirname)

	for _, e := range d.entries {
		e.validate(t, dirname)
	}
}

func f(name string, mode os.FileMode, age time.Duration, finalState entryFinalState) entry {
	return &fileEntry{name, mode, age, finalState}
}

func d(name string, mode os.FileMode, age time.Duration, finalState entryFinalState, entries []entry) entry {
	return &dirEntry{fileEntry{name, mode, age, finalState}, entries}
}

func TestPerform(t *testing.T) {
	testcases := []struct {
		name    string
		entries []entry
	}{
		{
			name: "clean",
			entries: []entry{
				d("objects", 0o700, 240*time.Hour, Keep, []entry{
					f("a", 0o700, 24*time.Hour, Keep),
					f("b", 0o700, 24*time.Hour, Keep),
					f("c", 0o700, 24*time.Hour, Keep),
				}),
			},
		},
		{
			name: "emptyperms",
			entries: []entry{
				d("objects", 0o700, 240*time.Hour, Keep, []entry{
					f("b", 0o700, 24*time.Hour, Keep),
					f("tmp_a", 0o000, 2*time.Hour, Keep),
				}),
			},
		},
		{
			name: "emptytempdir",
			entries: []entry{
				d("objects", 0o700, 240*time.Hour, Keep, []entry{
					d("tmp_d", 0o000, 240*time.Hour, Keep, []entry{}),
					f("b", 0o700, 24*time.Hour, Keep),
				}),
			},
		},
		{
			name: "oldtempfile",
			entries: []entry{
				d("objects", 0o700, 240*time.Hour, Keep, []entry{
					f("tmp_a", 0o770, 240*time.Hour, Delete),
					f("b", 0o700, 24*time.Hour, Keep),
				}),
			},
		},
		{
			name: "subdir temp file",
			entries: []entry{
				d("objects", 0o700, 240*time.Hour, Keep, []entry{
					d("a", 0o770, 240*time.Hour, Keep, []entry{
						f("tmp_b", 0o700, 240*time.Hour, Delete),
					}),
				}),
			},
		},
		{
			name: "inaccessible tmp directory",
			entries: []entry{
				d("objects", 0o700, 240*time.Hour, Keep, []entry{
					d("tmp_a", 0o000, 240*time.Hour, Keep, []entry{
						f("tmp_b", 0o700, 240*time.Hour, Delete),
					}),
				}),
			},
		},
		{
			name: "deeply nested inaccessible tmp directory",
			entries: []entry{
				d("objects", 0o700, 240*time.Hour, Keep, []entry{
					d("tmp_a", 0o700, 240*time.Hour, Keep, []entry{
						d("tmp_a", 0o700, 24*time.Hour, Keep, []entry{
							f("tmp_b", 0o000, 240*time.Hour, Delete),
						}),
					}),
				}),
			},
		},
		{
			name: "files outside of object database",
			entries: []entry{
				f("tmp_a", 0o770, 240*time.Hour, Keep),
				d("info", 0o700, 240*time.Hour, Keep, []entry{
					f("tmp_a", 0o770, 240*time.Hour, Keep),
				}),
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			ctx := testhelper.Context(t)

			// We need to fix permissions so we don't fail to
			// remove the temporary directory after the test.
			defer func() {
				require.NoError(t, FixDirectoryPermissions(ctx, repoPath))
			}()

			for _, e := range tc.entries {
				e.create(t, repoPath)
			}

			require.NoError(t, NewManager(nil).CleanStaleData(ctx, repo))

			for _, e := range tc.entries {
				e.validate(t, repoPath)
			}
		})
	}
}

func TestPerform_references(t *testing.T) {
	type ref struct {
		name string
		age  time.Duration
		size int
	}

	testcases := []struct {
		desc     string
		refs     []ref
		expected []string
	}{
		{
			desc: "normal reference",
			refs: []ref{
				{name: "refs/heads/master", age: 1 * time.Second, size: 40},
			},
			expected: []string{
				"refs/heads/master",
			},
		},
		{
			desc: "recent empty reference is not deleted",
			refs: []ref{
				{name: "refs/heads/master", age: 1 * time.Hour, size: 0},
			},
			expected: []string{
				"refs/heads/master",
			},
		},
		{
			desc: "old empty reference is deleted",
			refs: []ref{
				{name: "refs/heads/master", age: 25 * time.Hour, size: 0},
			},
			expected: nil,
		},
		{
			desc: "multiple references",
			refs: []ref{
				{name: "refs/keep/kept-because-recent", age: 1 * time.Hour, size: 0},
				{name: "refs/keep/kept-because-nonempty", age: 25 * time.Hour, size: 1},
				{name: "refs/keep/prune", age: 25 * time.Hour, size: 0},
				{name: "refs/tags/kept-because-recent", age: 1 * time.Hour, size: 0},
				{name: "refs/tags/kept-because-nonempty", age: 25 * time.Hour, size: 1},
				{name: "refs/tags/prune", age: 25 * time.Hour, size: 0},
				{name: "refs/heads/kept-because-recent", age: 1 * time.Hour, size: 0},
				{name: "refs/heads/kept-because-nonempty", age: 25 * time.Hour, size: 1},
				{name: "refs/heads/prune", age: 25 * time.Hour, size: 0},
			},
			expected: []string{
				"refs/keep/kept-because-recent",
				"refs/keep/kept-because-nonempty",
				"refs/tags/kept-because-recent",
				"refs/tags/kept-because-nonempty",
				"refs/heads/kept-because-recent",
				"refs/heads/kept-because-nonempty",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			for _, ref := range tc.refs {
				path := filepath.Join(repoPath, ref.name)

				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
				require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{0}, ref.size), 0o644))
				filetime := time.Now().Add(-ref.age)
				require.NoError(t, os.Chtimes(path, filetime, filetime))
			}
			ctx := testhelper.Context(t)

			require.NoError(t, NewManager(nil).CleanStaleData(ctx, repo))

			var actual []string
			require.NoError(t, filepath.Walk(filepath.Join(repoPath, "refs"), func(path string, info os.FileInfo, _ error) error {
				if !info.IsDir() {
					ref, err := filepath.Rel(repoPath, path)
					require.NoError(t, err)
					actual = append(actual, ref)
				}
				return nil
			}))

			require.ElementsMatch(t, tc.expected, actual)
		})
	}
}

func TestPerform_emptyRefDirs(t *testing.T) {
	testcases := []struct {
		name    string
		entries []entry
	}{
		{
			name: "unrelated empty directories",
			entries: []entry{
				d("objects", 0o700, 240*time.Hour, Keep, []entry{
					d("empty", 0o700, 240*time.Hour, Keep, []entry{}),
				}),
			},
		},
		{
			name: "empty ref dir gets retained",
			entries: []entry{
				d("refs", 0o700, 240*time.Hour, Keep, []entry{}),
			},
		},
		{
			name: "empty nested non-stale ref dir gets kept",
			entries: []entry{
				d("refs", 0o700, 240*time.Hour, Keep, []entry{
					d("nested", 0o700, 23*time.Hour, Keep, []entry{}),
				}),
			},
		},
		{
			name: "empty nested stale ref dir gets pruned",
			entries: []entry{
				d("refs", 0o700, 240*time.Hour, Keep, []entry{
					d("nested", 0o700, 240*time.Hour, Delete, []entry{}),
				}),
			},
		},
		{
			name: "hierarchy of nested stale ref dirs gets pruned",
			entries: []entry{
				d("refs", 0o700, 240*time.Hour, Keep, []entry{
					d("first", 0o700, 240*time.Hour, Delete, []entry{
						d("second", 0o700, 240*time.Hour, Delete, []entry{}),
					}),
				}),
			},
		},
		{
			name: "hierarchy with intermediate non-stale ref dir gets kept",
			entries: []entry{
				d("refs", 0o700, 240*time.Hour, Keep, []entry{
					d("first", 0o700, 240*time.Hour, Keep, []entry{
						d("second", 0o700, 1*time.Hour, Keep, []entry{
							d("third", 0o700, 24*time.Hour, Delete, []entry{}),
						}),
					}),
				}),
			},
		},
		{
			name: "stale hierrachy with refs gets partially retained",
			entries: []entry{
				d("refs", 0o700, 240*time.Hour, Keep, []entry{
					d("first", 0o700, 240*time.Hour, Keep, []entry{
						d("second", 0o700, 240*time.Hour, Delete, []entry{
							d("third", 0o700, 24*time.Hour, Delete, []entry{}),
						}),
						d("other", 0o700, 240*time.Hour, Keep, []entry{
							f("ref", 0o700, 1*time.Hour, Keep),
						}),
					}),
				}),
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)
			ctx := testhelper.Context(t)

			for _, e := range tc.entries {
				e.create(t, repoPath)
			}

			require.NoError(t, NewManager(nil).CleanStaleData(ctx, repo))

			for _, e := range tc.entries {
				e.validate(t, repoPath)
			}
		})
	}
}

func TestPerform_withSpecificFile(t *testing.T) {
	for file, finder := range map[string]staleFileFinderFn{
		"HEAD.lock":        findStaleLockfiles,
		"config.lock":      findStaleLockfiles,
		"packed-refs.lock": findPackedRefsLock,
		"packed-refs.new":  findPackedRefsNew,
	} {
		testPerformWithSpecificFile(t, file, finder)
	}
}

func testPerformWithSpecificFile(t *testing.T, file string, finder staleFileFinderFn) {
	ctx := testhelper.Context(t)

	cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	mgr := NewManager(nil)

	require.NoError(t, mgr.CleanStaleData(ctx, repo))
	for _, tc := range []struct {
		desc          string
		entries       []entry
		expectedFiles []string
	}{
		{
			desc: fmt.Sprintf("fresh %s is kept", file),
			entries: []entry{
				f(file, 0o700, 10*time.Minute, Keep),
			},
		},
		{
			desc: fmt.Sprintf("stale %s in subdir is kept", file),
			entries: []entry{
				d("subdir", 0o700, 240*time.Hour, Keep, []entry{
					f(file, 0o700, 24*time.Hour, Keep),
				}),
			},
		},
		{
			desc: fmt.Sprintf("stale %s is deleted", file),
			entries: []entry{
				f(file, 0o700, 61*time.Minute, Delete),
			},
			expectedFiles: []string{
				filepath.Join(repoPath, file),
			},
		},
		{
			desc: fmt.Sprintf("variations of %s are kept", file),
			entries: []entry{
				f(file[:len(file)-1], 0o700, 61*time.Minute, Keep),
				f("~"+file, 0o700, 61*time.Minute, Keep),
				f(file+"~", 0o700, 61*time.Minute, Keep),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			for _, e := range tc.entries {
				e.create(t, repoPath)
			}

			staleFiles, err := finder(ctx, repoPath)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.expectedFiles, staleFiles)

			require.NoError(t, mgr.CleanStaleData(ctx, repo))

			for _, e := range tc.entries {
				e.validate(t, repoPath)
			}
		})
	}
}

func TestPerform_referenceLocks(t *testing.T) {
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc                   string
		entries                []entry
		expectedReferenceLocks []string
	}{
		{
			desc: "fresh lock is kept",
			entries: []entry{
				d("refs", 0o755, 0*time.Hour, Keep, []entry{
					f("main", 0o755, 10*time.Minute, Keep),
					f("main.lock", 0o755, 10*time.Minute, Keep),
				}),
			},
		},
		{
			desc: "stale lock is deleted",
			entries: []entry{
				d("refs", 0o755, 0*time.Hour, Keep, []entry{
					f("main", 0o755, 1*time.Hour, Keep),
					f("main.lock", 0o755, 1*time.Hour, Delete),
				}),
			},
			expectedReferenceLocks: []string{
				"refs/main.lock",
			},
		},
		{
			desc: "nested reference locks are deleted",
			entries: []entry{
				d("refs", 0o755, 0*time.Hour, Keep, []entry{
					d("tags", 0o755, 0*time.Hour, Keep, []entry{
						f("main", 0o755, 1*time.Hour, Keep),
						f("main.lock", 0o755, 1*time.Hour, Delete),
					}),
					d("heads", 0o755, 0*time.Hour, Keep, []entry{
						f("main", 0o755, 1*time.Hour, Keep),
						f("main.lock", 0o755, 1*time.Hour, Delete),
					}),
					d("foobar", 0o755, 0*time.Hour, Keep, []entry{
						f("main", 0o755, 1*time.Hour, Keep),
						f("main.lock", 0o755, 1*time.Hour, Delete),
					}),
				}),
			},
			expectedReferenceLocks: []string{
				"refs/tags/main.lock",
				"refs/heads/main.lock",
				"refs/foobar/main.lock",
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			for _, e := range tc.entries {
				e.create(t, repoPath)
			}

			// We need to recreate the temporary directory on each
			// run, so we don't have the full path available when
			// creating the testcases.
			var expectedReferenceLocks []string
			for _, referenceLock := range tc.expectedReferenceLocks {
				expectedReferenceLocks = append(expectedReferenceLocks, filepath.Join(repoPath, referenceLock))
			}

			staleLockfiles, err := findStaleReferenceLocks(ctx, repoPath)
			require.NoError(t, err)
			require.ElementsMatch(t, expectedReferenceLocks, staleLockfiles)

			require.NoError(t, NewManager(nil).CleanStaleData(ctx, repo))

			for _, e := range tc.entries {
				e.validate(t, repoPath)
			}
		})
	}
}

func TestShouldRemoveTemporaryObject(t *testing.T) {
	type args struct {
		path    string
		modTime time.Time
		mode    os.FileMode
	}
	testcases := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "regular_file",
			args: args{
				path:    "/tmp/objects",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0o700,
			},
			want: false,
		},
		{
			name: "directory",
			args: args{
				path:    "/tmp/",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0o770,
			},
			want: false,
		},
		{
			name: "recent_time_file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0o600,
			},
			want: false,
		},
		{
			name: "old temp file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-8 * 24 * time.Hour),
				mode:    0o600,
			},
			want: true,
		},
		{
			name: "old_temp_file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0o000,
			},
			want: false,
		},
		{
			name: "inaccessible_recent_file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0o000,
			},
			want: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isStaleTemporaryObject(tc.args.path, tc.args.modTime, tc.args.mode))
		})
	}
}

func TestPerformRepoDoesNotExist(t *testing.T) {
	cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	ctx := testhelper.Context(t)

	require.NoError(t, os.RemoveAll(repoPath))

	require.NoError(t, NewManager(nil).CleanStaleData(ctx, repo))
}

func TestPerform_UnsetConfiguration(t *testing.T) {
	cfg := testcfg.Build(t)
	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	configPath := filepath.Join(repoPath, "config")

	ctx := testhelper.Context(t)

	require.NoError(t, os.WriteFile(configPath, []byte(
		`[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
	commitGraph = true
	sparseCheckout = true
	splitIndex = false
[remote "first"]
	fetch = baz
	mirror = baz
	prune = baz
	url = baz
[http "first"]
	extraHeader = barfoo
[http "second"]
	extraHeader = barfoo
[http]
	extraHeader = untouched
[http "something"]
	else = untouched
[totally]
	unrelated = untouched
`), 0o644))

	require.NoError(t, NewManager(nil).CleanStaleData(ctx, repo))
	require.Equal(t,
		`[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
[http]
	extraHeader = untouched
[http "something"]
	else = untouched
[totally]
	unrelated = untouched
`, string(testhelper.MustReadFile(t, configPath)))
}

func TestPerform_UnsetConfiguration_transactional(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.some.extraHeader", "value")

	txManager := transaction.NewTrackingManager()

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = peer.NewContext(ctx, &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	})

	require.NoError(t, NewManager(txManager).CleanStaleData(ctx, repo))
	require.Equal(t, 2, len(txManager.Votes()))

	configKeys := gittest.Exec(t, cfg, "-C", repoPath, "config", "--list", "--local", "--name-only")

	expectedConfig := "core.repositoryformatversion\ncore.filemode\ncore.bare\n"

	if runtime.GOOS == "darwin" {
		expectedConfig = expectedConfig + "core.ignorecase\ncore.precomposeunicode\n"
	}
	require.Equal(t, expectedConfig, string(configKeys))
}

func TestPerform_cleanupConfig(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	configPath := filepath.Join(repoPath, "config")

	require.NoError(t, os.WriteFile(configPath, []byte(
		`[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
[uploadpack]
	allowAnySHA1InWant = true
[remote "tmp-8be1695862b62390d1f873f9164122e4"]
[remote "tmp-d97f78c39fde4b55e0d0771dfc0501ef"]
[remote "tmp-23a2471e7084e1548ef47bbc9d6afff6"]
[remote "tmp-d76633a16d61f6681de396ec9ecfd7b5"]
	prune = true
[remote "tmp-8fbf8d5e7585d48668f1791284a912ef"]
[remote "tmp-f539c59068f291e52f1140e39830f9ca"]
[remote "tmp-17b67d28909768db3213917255c72af2"]
	prune = true
[remote "tmp-03b5e8c765135b343214d471843a062a"]
[remote "tmp-f57338181aca1d599669dbb71ce9ce57"]
[remote "tmp-8c948ca94832c2725733e48cb2902287"]
`), 0o644))

	require.NoError(t, NewManager(nil).CleanStaleData(ctx, repo))
	require.Equal(t, `[core]
	repositoryformatversion = 0
	filemode = true
	bare = true
[uploadpack]
	allowAnySHA1InWant = true
`, string(testhelper.MustReadFile(t, configPath)))
}

func TestPruneEmptyConfigSections(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)
	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	configPath := filepath.Join(repoPath, "config")

	for _, tc := range []struct {
		desc         string
		configData   string
		expectedData string
	}{
		{
			desc:         "empty",
			configData:   "",
			expectedData: "",
		},
		{
			desc:         "newline only",
			configData:   "\n",
			expectedData: "\n",
		},
		{
			desc:         "no stripping",
			configData:   "[foo]\nbar = baz\n",
			expectedData: "[foo]\nbar = baz\n",
		},
		{
			desc:         "no stripping with missing newline",
			configData:   "[foo]\nbar = baz",
			expectedData: "[foo]\nbar = baz",
		},
		{
			desc:         "multiple sections",
			configData:   "[foo]\nbar = baz\n[bar]\nfoo = baz\n",
			expectedData: "[foo]\nbar = baz\n[bar]\nfoo = baz\n",
		},
		{
			desc:         "missing newline",
			configData:   "[foo]\nbar = baz",
			expectedData: "[foo]\nbar = baz",
		},
		{
			desc:         "single comment",
			configData:   "# foobar\n",
			expectedData: "# foobar\n",
		},
		{
			// This is not correct, but we really don't want to start parsing
			// the config format completely. So we err on the side of caution
			// and just say this is fine.
			desc:         "empty section with comment",
			configData:   "[foo]\n# comment\n[bar]\n[baz]\n",
			expectedData: "[foo]\n# comment\n",
		},
		{
			desc:         "empty section",
			configData:   "[foo]\n",
			expectedData: "",
		},
		{
			desc:         "empty sections",
			configData:   "[foo]\n[bar]\n[baz]\n",
			expectedData: "",
		},
		{
			desc:         "empty sections with missing newline",
			configData:   "[foo]\n[bar]\n[baz]",
			expectedData: "",
		},
		{
			desc:         "trailing empty section",
			configData:   "[foo]\nbar = baz\n[foo]\n",
			expectedData: "[foo]\nbar = baz\n",
		},
		{
			desc:         "mixed keys and sections",
			configData:   "[empty]\n[nonempty]\nbar = baz\nbar = baz\n[empty]\n",
			expectedData: "[nonempty]\nbar = baz\nbar = baz\n",
		},
		{
			desc: "real world example",
			configData: `[core]
        repositoryformatversion = 0
        filemode = true
        bare = true
[uploadpack]
        allowAnySHA1InWant = true
[remote "tmp-8be1695862b62390d1f873f9164122e4"]
[remote "tmp-d97f78c39fde4b55e0d0771dfc0501ef"]
[remote "tmp-23a2471e7084e1548ef47bbc9d6afff6"]
[remote "tmp-6ef9759bb14db34ca67de4681f0a812a"]
[remote "tmp-992cb6a0ea428a511cc2de3cde051227"]
[remote "tmp-a720c2b6794fdbad50f36f0a4e9501ff"]
[remote "tmp-4b4f6d68031aa1288613f40b1a433278"]
[remote "tmp-fc12da796c907e8ea5faed134806acfb"]
[remote "tmp-49e1fbb6eccdb89059a7231eef785d03"]
[remote "tmp-e504bbbed5d828cd96b228abdef4b055"]
[remote "tmp-36e856371fdacb7b4909240ba6bc0b34"]
[remote "tmp-9a1bc23bb2200b9426340a5ba934f5ba"]
[remote "tmp-49ead30f732995498e0585b569917c31"]
[remote "tmp-8419f1e1445ccd6e1c60aa421573447c"]
[remote "tmp-f7a91ec9415f984d3747cf608b0a7e9c"]
        prune = true
[remote "tmp-ea77d1e5348d07d693aa2bf8a2c98637"]
[remote "tmp-3f190ab463b804612cb007487e0cbb4d"]`,
			expectedData: `[core]
        repositoryformatversion = 0
        filemode = true
        bare = true
[uploadpack]
        allowAnySHA1InWant = true
[remote "tmp-f7a91ec9415f984d3747cf608b0a7e9c"]
        prune = true
`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.NoError(t, os.WriteFile(configPath, []byte(tc.configData), 0o644))
			require.NoError(t, pruneEmptyConfigSections(ctx, repo))
			require.Equal(t, tc.expectedData, string(testhelper.MustReadFile(t, configPath)))
		})
	}
}
