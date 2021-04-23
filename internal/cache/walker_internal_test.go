package cache

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestCleanWalkDirNotExists(t *testing.T) {
	err := cleanWalk("/path/that/does/not/exist")
	assert.NoError(t, err, "cleanWalk returned an error for a non existing directory")
}

func TestCleanWalkEmptyDirs(t *testing.T) {
	tmp := testhelper.TempDir(t)

	for _, tt := range []struct {
		path  string
		stale bool
	}{
		{path: "a/b/c/"},
		{path: "a/b/c/1", stale: true},
		{path: "a/b/c/2", stale: true},
		{path: "a/b/d/"},
		{path: "e/"},
		{path: "e/1"},
		{path: "f/"},
	} {
		p := filepath.Join(tmp, tt.path)
		if strings.HasSuffix(tt.path, "/") {
			require.NoError(t, os.MkdirAll(p, 0755))
		} else {
			require.NoError(t, ioutil.WriteFile(p, nil, 0655))
			if tt.stale {
				require.NoError(t, os.Chtimes(p, time.Now(), time.Now().Add(-time.Hour)))
			}
		}
	}

	require.NoError(t, cleanWalk(tmp))

	actual := findFiles(t, tmp)
	expect := `.
./e
./e/1
`
	require.Equal(t, expect, actual)
}

func findFiles(t testing.TB, path string) string {
	cmd := exec.Command("find", ".")
	cmd.Dir = path
	out, err := cmd.Output()
	require.NoError(t, err)
	return string(out)
}
