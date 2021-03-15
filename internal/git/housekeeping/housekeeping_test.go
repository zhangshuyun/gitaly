package housekeeping

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
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
	ff, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0700)
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

	if err := os.Mkdir(dirname, 0700); err != nil {
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
				d("objects", 0700, 240*time.Hour, Keep, []entry{
					f("a", 0700, 24*time.Hour, Keep),
					f("b", 0700, 24*time.Hour, Keep),
					f("c", 0700, 24*time.Hour, Keep),
				}),
			},
		},
		{
			name: "emptyperms",
			entries: []entry{
				d("objects", 0700, 240*time.Hour, Keep, []entry{
					f("b", 0700, 24*time.Hour, Keep),
					f("tmp_a", 0000, 2*time.Hour, Keep),
				}),
			},
		},
		{
			name: "emptytempdir",
			entries: []entry{
				d("objects", 0700, 240*time.Hour, Keep, []entry{
					d("tmp_d", 0000, 240*time.Hour, Keep, []entry{}),
					f("b", 0700, 24*time.Hour, Keep),
				}),
			},
		},
		{
			name: "oldtempfile",
			entries: []entry{
				d("objects", 0700, 240*time.Hour, Keep, []entry{
					f("tmp_a", 0770, 240*time.Hour, Delete),
					f("b", 0700, 24*time.Hour, Keep),
				}),
			},
		},
		{
			name: "subdir temp file",
			entries: []entry{
				d("objects", 0700, 240*time.Hour, Keep, []entry{
					d("a", 0770, 240*time.Hour, Keep, []entry{
						f("tmp_b", 0700, 240*time.Hour, Delete),
					}),
				}),
			},
		},
		{
			name: "inaccessible tmp directory",
			entries: []entry{
				d("objects", 0700, 240*time.Hour, Keep, []entry{
					d("tmp_a", 0000, 240*time.Hour, Keep, []entry{
						f("tmp_b", 0700, 240*time.Hour, Delete),
					}),
				}),
			},
		},
		{
			name: "deeply nested inaccessible tmp directory",
			entries: []entry{
				d("objects", 0700, 240*time.Hour, Keep, []entry{
					d("tmp_a", 0700, 240*time.Hour, Keep, []entry{
						d("tmp_a", 0700, 24*time.Hour, Keep, []entry{
							f("tmp_b", 0000, 240*time.Hour, Delete),
						}),
					}),
				}),
			},
		},
		{
			name: "files outside of object database",
			entries: []entry{
				f("tmp_a", 0770, 240*time.Hour, Keep),
				d("info", 0700, 240*time.Hour, Keep, []entry{
					f("tmp_a", 0770, 240*time.Hour, Keep),
				}),
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
			repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

			ctx, cancel := testhelper.Context()
			defer cancel()

			// We need to fix permissions so we don't fail to
			// remove the temporary directory after the test.
			defer FixDirectoryPermissions(ctx, repoPath)

			for _, e := range tc.entries {
				e.create(t, repoPath)
			}

			require.NoError(t, Perform(ctx, repo))

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
			repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

			for _, ref := range tc.refs {
				path := filepath.Join(repoPath, ref.name)

				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
				require.NoError(t, ioutil.WriteFile(path, bytes.Repeat([]byte{0}, ref.size), 0644))
				filetime := time.Now().Add(-ref.age)
				require.NoError(t, os.Chtimes(path, filetime, filetime))
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			require.NoError(t, Perform(ctx, repo))

			var actual []string
			filepath.Walk(filepath.Join(repoPath, "refs"), func(path string, info os.FileInfo, _ error) error {
				if !info.IsDir() {
					ref, err := filepath.Rel(repoPath, path)
					require.NoError(t, err)
					actual = append(actual, ref)
				}
				return nil
			})

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
				d("objects", 0700, 240*time.Hour, Keep, []entry{
					d("empty", 0700, 240*time.Hour, Keep, []entry{}),
				}),
			},
		},
		{
			name: "empty ref dir gets retained",
			entries: []entry{
				d("refs", 0700, 240*time.Hour, Keep, []entry{}),
			},
		},
		{
			name: "empty nested non-stale ref dir gets kept",
			entries: []entry{
				d("refs", 0700, 240*time.Hour, Keep, []entry{
					d("nested", 0700, 23*time.Hour, Keep, []entry{}),
				}),
			},
		},
		{
			name: "empty nested stale ref dir gets pruned",
			entries: []entry{
				d("refs", 0700, 240*time.Hour, Keep, []entry{
					d("nested", 0700, 240*time.Hour, Delete, []entry{}),
				}),
			},
		},
		{
			name: "hierarchy of nested stale ref dirs gets pruned",
			entries: []entry{
				d("refs", 0700, 240*time.Hour, Keep, []entry{
					d("first", 0700, 240*time.Hour, Delete, []entry{
						d("second", 0700, 240*time.Hour, Delete, []entry{}),
					}),
				}),
			},
		},
		{
			name: "hierarchy with intermediate non-stale ref dir gets kept",
			entries: []entry{
				d("refs", 0700, 240*time.Hour, Keep, []entry{
					d("first", 0700, 240*time.Hour, Keep, []entry{
						d("second", 0700, 1*time.Hour, Keep, []entry{
							d("third", 0700, 24*time.Hour, Delete, []entry{}),
						}),
					}),
				}),
			},
		},
		{
			name: "stale hierrachy with refs gets partially retained",
			entries: []entry{
				d("refs", 0700, 240*time.Hour, Keep, []entry{
					d("first", 0700, 240*time.Hour, Keep, []entry{
						d("second", 0700, 240*time.Hour, Delete, []entry{
							d("third", 0700, 24*time.Hour, Delete, []entry{}),
						}),
						d("other", 0700, 240*time.Hour, Keep, []entry{
							f("ref", 0700, 1*time.Hour, Keep),
						}),
					}),
				}),
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
			repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

			ctx, cancel := testhelper.Context()
			defer cancel()

			for _, e := range tc.entries {
				e.create(t, repoPath)
			}

			require.NoError(t, Perform(ctx, repo))

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
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	for _, tc := range []struct {
		desc          string
		entries       []entry
		expectedFiles []string
	}{
		{
			desc: fmt.Sprintf("fresh %s is kept", file),
			entries: []entry{
				f(file, 0700, 10*time.Minute, Keep),
			},
		},
		{
			desc: fmt.Sprintf("stale %s in subdir is kept", file),
			entries: []entry{
				d("subdir", 0700, 240*time.Hour, Keep, []entry{
					f(file, 0700, 24*time.Hour, Keep),
				}),
			},
		},
		{
			desc: fmt.Sprintf("stale %s is deleted", file),
			entries: []entry{
				f(file, 0700, 61*time.Minute, Delete),
			},
			expectedFiles: []string{
				filepath.Join(repoPath, file),
			},
		},
		{
			desc: fmt.Sprintf("variations of %s are kept", file),
			entries: []entry{
				f(file[:len(file)-1], 0700, 61*time.Minute, Keep),
				f("~"+file, 0700, 61*time.Minute, Keep),
				f(file+"~", 0700, 61*time.Minute, Keep),
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

			require.NoError(t, Perform(ctx, repo))

			for _, e := range tc.entries {
				e.validate(t, repoPath)
			}
		})
	}
}

func TestPerform_referenceLocks(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range []struct {
		desc                   string
		entries                []entry
		expectedReferenceLocks []string
	}{
		{
			desc: "fresh lock is kept",
			entries: []entry{
				d("refs", 0755, 0*time.Hour, Keep, []entry{
					f("main", 0755, 10*time.Minute, Keep),
					f("main.lock", 0755, 10*time.Minute, Keep),
				}),
			},
		},
		{
			desc: "stale lock is deleted",
			entries: []entry{
				d("refs", 0755, 0*time.Hour, Keep, []entry{
					f("main", 0755, 1*time.Hour, Keep),
					f("main.lock", 0755, 1*time.Hour, Delete),
				}),
			},
			expectedReferenceLocks: []string{
				"refs/main.lock",
			},
		},
		{
			desc: "nested reference locks are deleted",
			entries: []entry{
				d("refs", 0755, 0*time.Hour, Keep, []entry{
					d("tags", 0755, 0*time.Hour, Keep, []entry{
						f("main", 0755, 1*time.Hour, Keep),
						f("main.lock", 0755, 1*time.Hour, Delete),
					}),
					d("heads", 0755, 0*time.Hour, Keep, []entry{
						f("main", 0755, 1*time.Hour, Keep),
						f("main.lock", 0755, 1*time.Hour, Delete),
					}),
					d("foobar", 0755, 0*time.Hour, Keep, []entry{
						f("main", 0755, 1*time.Hour, Keep),
						f("main.lock", 0755, 1*time.Hour, Delete),
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
			repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

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

			require.NoError(t, Perform(ctx, repo))

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
				mode:    0700,
			},
			want: false,
		},
		{
			name: "directory",
			args: args{
				path:    "/tmp/",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0770,
			},
			want: false,
		},
		{
			name: "recent_time_file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0600,
			},
			want: false,
		},
		{
			name: "old temp file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-8 * 24 * time.Hour),
				mode:    0600,
			},
			want: true,
		},
		{
			name: "old_temp_file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0000,
			},
			want: false,
		},
		{
			name: "inaccessible_recent_file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0000,
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
	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	ctx, cancel := testhelper.Context()
	defer cancel()

	require.NoError(t, os.RemoveAll(repoPath))

	require.NoError(t, Perform(ctx, repo))
}

func TestPerform_UnsetConfiguration(t *testing.T) {
	cfg, repoProto, _ := testcfg.BuildWithRepo(t)
	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	ctx, cancel := testhelper.Context()
	defer cancel()

	for key, value := range map[string]string{
		"http.first.extraHeader":  "barfoo",
		"http.second.extraHeader": "barfoo",
		"http.extraHeader":        "untouched",
		"http.something.else":     "untouched",
		"totally.unrelated":       "untouched",
	} {
		require.NoError(t, repo.Config().Add(ctx, key, value, git.ConfigAddOpts{}))
	}

	opts, err := repo.Config().GetRegexp(ctx, ".*", git.ConfigGetRegexpOpts{})
	require.NoError(t, err)

	var filteredOpts []git.ConfigPair
	for _, opt := range opts {
		key := strings.ToLower(opt.Key)
		if key != "http.first.extraheader" && key != "http.second.extraheader" {
			filteredOpts = append(filteredOpts, opt)
		}
	}

	require.NoError(t, Perform(ctx, repo))

	opts, err = repo.Config().GetRegexp(ctx, ".*", git.ConfigGetRegexpOpts{})
	require.NoError(t, err)
	require.Equal(t, filteredOpts, opts)
}
