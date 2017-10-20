package housekeeping

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
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
	tests := []struct {
		name    string
		entries []entry
		wantErr bool
	}{
		{
			name: "clean",
			entries: []entry{
				f("a", os.FileMode(0700), 24*time.Hour, Keep),
				f("b", os.FileMode(0700), 24*time.Hour, Keep),
				f("c", os.FileMode(0700), 24*time.Hour, Keep),
			},
			wantErr: false,
		},
		{
			name: "emptyperms",
			entries: []entry{
				f("b", os.FileMode(0700), 24*time.Hour, Keep),
				f("tmp_a", os.FileMode(0000), 2*time.Hour, Keep),
			},
			wantErr: false,
		},
		{
			name: "emptytempdir",
			entries: []entry{
				d("tmp_d", os.FileMode(0000), 240*time.Hour, Delete, []entry{}),
				f("b", os.FileMode(0700), 24*time.Hour, Keep),
			},
			wantErr: false,
		},
		{
			name: "oldtempfile",
			entries: []entry{
				f("tmp_a", os.FileMode(0770), 240*time.Hour, Delete),
				f("b", os.FileMode(0700), 24*time.Hour, Keep),
			},
			wantErr: false,
		},
		{
			name: "subdir temp file",
			entries: []entry{
				d("a", os.FileMode(0770), 240*time.Hour, Keep, []entry{
					f("tmp_b", os.FileMode(0700), 240*time.Hour, Delete),
				}),
			},
			wantErr: false,
		},
		{
			name: "inaccessible tmp directory",
			entries: []entry{
				d("tmp_a", os.FileMode(0000), 240*time.Hour, Delete, []entry{
					f("tmp_b", os.FileMode(0700), 240*time.Hour, Delete),
				}),
			},
			wantErr: false,
		},
		{
			name: "deeply nested inaccessible tmp directory",
			entries: []entry{
				d("tmp_a", os.FileMode(0000), 240*time.Hour, Delete, []entry{
					d("tmp_a", os.FileMode(0000), 24*time.Hour, Delete, []entry{
						f("tmp_b", os.FileMode(0000), 24*time.Hour, Delete),
					}),
				}),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootPath, err := ioutil.TempDir("", "test")
			assert.NoError(t, err, "TempDir creation failed")
			defer os.RemoveAll(rootPath)

			for _, e := range tt.entries {
				e.create(t, rootPath)
			}

			if err = Perform(context.Background(), rootPath); (err != nil) != tt.wantErr {
				t.Errorf("Perform() error = %v, wantErr %v", err, tt.wantErr)
			}

			for _, e := range tt.entries {
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
		err     error
	}
	tests := []struct {
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
				err:     nil,
			},
			want: false,
		},
		{
			name: "directory",
			args: args{
				path:    "/tmp/",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0770,
				err:     nil,
			},
			want: false,
		},
		{
			name: "recent_time_file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0600,
				err:     nil,
			},
			want: false,
		},
		{
			name: "old temp file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-8 * 24 * time.Hour),
				mode:    0600,
				err:     nil,
			},
			want: true,
		},
		{
			name: "old_temp_file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0000,
				err:     nil,
			},
			want: false,
		},
		{
			name: "inaccessible_recent_file",
			args: args{
				path:    "/tmp/tmp_DELETEME",
				modTime: time.Now().Add(-1 * time.Hour),
				mode:    0000,
				err:     fmt.Errorf("file inaccessible"),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRemove(tt.args.path, tt.args.modTime, tt.args.mode, tt.args.err); got != tt.want {
				t.Errorf("shouldUnlink() = %v, want %v", got, tt.want)
			}
		})
	}
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

	err := Perform(context.Background(), rootPath)
	assert.NoError(t, err, "Housekeeping failed")

	_, err = os.Stat(filepath.Join(rootPath, "tmp_FILE"))
	assert.Error(t, err, "Expected tmp_FILE to be missing")

	_, err = os.Stat(filepath.Join(rootPath, "tmp_DIR"))
	assert.Error(t, err, "Expected tmp_DIR to be missing")

	_, err = os.Stat(filepath.Join(rootPath, "tmp_DIR2"))
	assert.Error(t, err, "Expected tmp_DIR2 to be missing")
}
