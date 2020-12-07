package housekeeping

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
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
	err := os.Mkdir(dirname, 0700)
	assert.NoError(t, err, "mkdir failed: %v", dirname)

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
				f("a", 0700, 24*time.Hour, Keep),
				f("b", 0700, 24*time.Hour, Keep),
				f("c", 0700, 24*time.Hour, Keep),
			},
		},
		{
			name: "emptyperms",
			entries: []entry{
				f("b", 0700, 24*time.Hour, Keep),
				f("tmp_a", 0000, 2*time.Hour, Keep),
			},
		},
		{
			name: "emptytempdir",
			entries: []entry{
				d("tmp_d", 0000, 240*time.Hour, Delete, []entry{}),
				f("b", 0700, 24*time.Hour, Keep),
			},
		},
		{
			name: "oldtempfile",
			entries: []entry{
				f("tmp_a", 0770, 240*time.Hour, Delete),
				f("b", 0700, 24*time.Hour, Keep),
			},
		},
		{
			name: "subdir temp file",
			entries: []entry{
				d("a", 0770, 240*time.Hour, Keep, []entry{
					f("tmp_b", 0700, 240*time.Hour, Delete),
				}),
			},
		},
		{
			name: "inaccessible tmp directory",
			entries: []entry{
				d("tmp_a", 0000, 240*time.Hour, Delete, []entry{
					f("tmp_b", 0700, 240*time.Hour, Delete),
				}),
			},
		},
		{
			name: "deeply nested inaccessible tmp directory",
			entries: []entry{
				d("tmp_a", 0000, 240*time.Hour, Delete, []entry{
					d("tmp_a", 0000, 24*time.Hour, Delete, []entry{
						f("tmp_b", 0000, 24*time.Hour, Delete),
					}),
				}),
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			rootPath, cleanup := testhelper.TempDir(t)
			defer cleanup()

			for _, e := range tc.entries {
				e.create(t, rootPath)
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			require.NoError(t, Perform(ctx, rootPath))

			for _, e := range tc.entries {
				e.validate(t, rootPath)
			}
		})
	}
}

func TestShouldUnlink(t *testing.T) {
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
			require.Equal(t, tc.want, shouldRemove(tc.args.path, tc.args.modTime, tc.args.mode))
		})
	}
}

func TestPerformRepoDoesNotExist(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()
	require.NoError(t, Perform(ctx, "/does/not/exist"))
}

// This test exists only ever for manual testing purposes.
// Set it up as follows:
/*
export TEST_DELETE_ROOT_OWNER_DIR=$(mktemp -d)
sudo mkdir "${TEST_DELETE_ROOT_OWNER_DIR}/tmp_DIR"
sudo touch -t 1201010000.00 "${TEST_DELETE_ROOT_OWNER_DIR}/tmp_FILE" "${TEST_DELETE_ROOT_OWNER_DIR}/tmp_DIR"
sudo chmod 000 "${TEST_DELETE_ROOT_OWNER_DIR}/tmp_DIR" "${TEST_DELETE_ROOT_OWNER_DIR}/tmp_FILE"
sudo touch -t 1201010000.00 "${TEST_DELETE_ROOT_OWNER_DIR}/tmp_DIR"
mkdir -p "${TEST_DELETE_ROOT_OWNER_DIR}/tmp_DIR2/a/b"
touch "${TEST_DELETE_ROOT_OWNER_DIR}/tmp_DIR2/a/b/c"
chmod 000 $(find ${TEST_DELETE_ROOT_OWNER_DIR}/tmp_DIR2|sort -r)
go test ./internal/helper/housekeeping/... -v -run 'TestDeleteRootOwnerObjects'
*/
func TestDeleteRootOwnerObjects(t *testing.T) {
	rootPath := os.Getenv("TEST_DELETE_ROOT_OWNER_DIR")
	if rootPath == "" {
		t.Skip("skipping test; Only used for manual testing")
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	err := Perform(ctx, rootPath)
	assert.NoError(t, err, "Housekeeping failed")

	_, err = os.Stat(filepath.Join(rootPath, "tmp_FILE"))
	assert.Error(t, err, "Expected tmp_FILE to be missing")

	_, err = os.Stat(filepath.Join(rootPath, "tmp_DIR"))
	assert.Error(t, err, "Expected tmp_DIR to be missing")

	_, err = os.Stat(filepath.Join(rootPath, "tmp_DIR2"))
	assert.Error(t, err, "Expected tmp_DIR2 to be missing")
}
