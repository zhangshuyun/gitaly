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
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
)

func TestDiskCacheObjectWalker(t *testing.T) {
	// disable the initial move-and-clear function since we are only
	// evaluating the walker
	*cache.ExportDisableMoveAndClear = true
	defer func() { *cache.ExportDisableMoveAndClear = false }()

	cfg := testcfg.Build(t)

	var shouldExist, shouldNotExist []string

	cache.ExportMockRemovalCounter.Reset()

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
		cacheDir := tempdir.CacheDir(cfg.Storages[0])

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

	require.NoError(t, cfg.Validate()) // triggers walker

	pollCountersUntil(t, 4)

	for _, p := range shouldExist {
		assert.FileExists(t, p)
	}

	for _, p := range shouldNotExist {
		require.NoFileExists(t, p)
	}
}

func TestDiskCacheInitialClear(t *testing.T) {
	// disable the background walkers since we are only
	// evaluating the initial move-and-clear function
	*cache.ExportDisableWalker = true
	defer func() { *cache.ExportDisableWalker = false }()

	cfg := testcfg.Build(t)

	cacheDir := tempdir.CacheDir(cfg.Storages[0])

	canary := filepath.Join(cacheDir, "canary.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(canary), 0755))
	require.NoError(t, ioutil.WriteFile(canary, []byte("chirp chirp"), 0755))

	// validation will run cache walker hook which synchronously
	// runs the move-and-clear function
	require.NoError(t, cfg.Validate())

	require.NoFileExists(t, canary)
}

func pollCountersUntil(t testing.TB, expectRemovals int) {
	// poll injected mock prometheus counters until expected events occur
	timeout := time.After(time.Second)
	for {
		count := cache.ExportMockRemovalCounter.Count()
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
