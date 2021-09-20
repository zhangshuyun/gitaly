package cache

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestDiskCacheObjectWalker(t *testing.T) {
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)

	var shouldExist, shouldNotExist []string

	for _, tt := range []struct {
		name          string
		age           time.Duration
		expectRemoval bool
	}{
		{"0f/oldey", time.Hour, true},
		{"90/n00b", time.Minute, false},
		{"2b/ancient", 24 * time.Hour, true},
		{"cd/baby", time.Second, false},
	} {
		cacheDir, err := locator.CacheDir(cfg.Storages[0].Name)
		require.NoError(t, err)

		path := filepath.Join(cacheDir, tt.name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))

		f, err := os.Create(path)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		require.NoError(t, os.Chtimes(path, time.Now(), time.Now().Add(-1*tt.age)))

		if tt.expectRemoval {
			shouldNotExist = append(shouldNotExist, path)
		} else {
			shouldExist = append(shouldExist, path)
		}
	}

	cache := New(cfg, locator, withDisabledMoveAndClear())
	require.NoError(t, cache.StartWalkers())

	pollCountersUntil(t, cache, 4)

	for _, p := range shouldExist {
		assert.FileExists(t, p)
	}

	for _, p := range shouldNotExist {
		require.NoFileExists(t, p)
	}
}

func TestDiskCacheInitialClear(t *testing.T) {
	cfg := testcfg.Build(t)
	locator := config.NewLocator(cfg)

	cacheDir, err := locator.CacheDir(cfg.Storages[0].Name)
	require.NoError(t, err)

	canary := filepath.Join(cacheDir, "canary.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(canary), 0o755))
	require.NoError(t, os.WriteFile(canary, []byte("chirp chirp"), 0o755))

	cache := New(cfg, locator, withDisabledWalker())
	require.NoError(t, cache.StartWalkers())

	require.NoFileExists(t, canary)
}

func pollCountersUntil(t testing.TB, cache *DiskCache, expectRemovals int) {
	// poll injected mock prometheus counters until expected events occur
	timeout := time.After(time.Second)
	for {
		count := int(promtest.ToFloat64(cache.walkerRemovalTotal))
		select {
		case <-timeout:
			t.Fatalf(
				"timed out polling prometheus stats; removals: %d",
				count,
			)
		default:
			// keep on truckin'
		}
		if count == expectRemovals {
			break
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCleanWalkDirNotExists(t *testing.T) {
	cfg := testcfg.Build(t)

	cache := New(cfg, config.NewLocator(cfg))

	err := cache.cleanWalk("/path/that/does/not/exist")
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
			require.NoError(t, os.MkdirAll(p, 0o755))
		} else {
			require.NoError(t, os.WriteFile(p, nil, 0o655))
			if tt.stale {
				require.NoError(t, os.Chtimes(p, time.Now(), time.Now().Add(-time.Hour)))
			}
		}
	}

	cfg := testcfg.Build(t)
	cache := New(cfg, config.NewLocator(cfg))

	require.NoError(t, cache.cleanWalk(tmp))

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
