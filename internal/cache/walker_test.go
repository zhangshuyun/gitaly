package cache_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestDiskCacheObjectWalker(t *testing.T) {
	cleanup := setupDiskCacheWalker(t)
	defer cleanup()

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
		cacheDir, err := tempdir.CacheDir(t.Name()) // test name is storage name
		require.NoError(t, err)

		path := filepath.Join(cacheDir, tt.name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))

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

	expectChecks := cache.ExportMockCheckCounter.Count() + 4
	expectRemovals := cache.ExportMockRemovalCounter.Count() + 2

	// disable the initial move-and-clear function since we are only
	// evaluating the walker
	*cache.ExportDisableMoveAndClear = true
	defer func() { *cache.ExportDisableMoveAndClear = false }()

	cache.StartCleaning()

	pollCountersUntil(t, expectChecks, expectRemovals)

	for _, p := range shouldExist {
		assert.FileExists(t, p)
	}

	for _, p := range shouldNotExist {
		_, err := os.Stat(p)
		require.True(t, os.IsNotExist(err), "expected %s not to exist", p)
	}
}

func TestDiskCacheInitialClear(t *testing.T) {
	cleanup := setupDiskCacheWalker(t)
	defer cleanup()

	cacheDir, err := tempdir.CacheDir(t.Name()) // test name is storage name
	require.NoError(t, err)

	canary := filepath.Join(cacheDir, "canary.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(canary), 0755))
	require.NoError(t, ioutil.WriteFile(canary, []byte("chirp chirp"), 0755))

	// disable the background walkers since we are only
	// evaluating the initial move-and-clear function
	*cache.ExportDisableWalker = true
	defer func() { *cache.ExportDisableWalker = false }()

	// validation will run cache walker hook which synchronously
	// runs the move-and-clear function
	require.NoError(t, config.Validate())

	testhelper.AssertFileNotExists(t, canary)
}

func setupDiskCacheWalker(t testing.TB) func() {
	tmpPath, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)

	oldStorages := config.Config.Storages
	config.Config.Storages = []config.Storage{
		{
			Name: t.Name(),
			Path: tmpPath,
		},
	}

	satisfyConfigValidation(tmpPath)

	cleanup := func() {
		config.Config.Storages = oldStorages
		require.NoError(t, os.RemoveAll(tmpPath))
	}

	return cleanup
}

// satisfyConfigValidation puts garbage values in the config file to satisfy
// validation
func satisfyConfigValidation(tmpPath string) {
	config.Config.ListenAddr = "meow"
	config.Config.GitlabShell = config.GitlabShell{
		Dir: tmpPath,
	}
	config.Config.Ruby = config.Ruby{
		Dir: tmpPath,
	}
}

func pollCountersUntil(t testing.TB, expectChecks, expectRemovals int) {
	// poll injected mock prometheus counters until expected events occur
	timeout := time.After(time.Second)
	for {
		select {
		case <-timeout:
			t.Fatalf(
				"timed out polling prometheus stats; checks: %d removals: %d",
				cache.ExportMockCheckCounter.Count(),
				cache.ExportMockRemovalCounter.Count(),
			)
		default:
			// keep on truckin'
		}
		if cache.ExportMockCheckCounter.Count() == expectChecks &&
			cache.ExportMockRemovalCounter.Count() == expectRemovals {
			break
		}
		time.Sleep(time.Millisecond)
	}
}
