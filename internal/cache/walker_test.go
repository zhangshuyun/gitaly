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
		cacheDir := tempdir.CacheDir(config.Config.Storages[0])

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

	require.NoError(t, config.Validate()) // triggers walker

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

	cacheDir := tempdir.CacheDir(config.Config.Storages[0])

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

	testhelper.AssertPathNotExists(t, canary)
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
func satisfyConfigValidation(tmpPath string) error {
	config.Config.ListenAddr = "meow"

	if err := os.MkdirAll(filepath.Join(tmpPath, "hooks"), 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(tmpPath, "git-hooks"), 0700); err != nil {
		return err
	}

	for _, filePath := range []string{
		filepath.Join("ruby", "git-hooks", "pre-receive"),
		filepath.Join("ruby", "git-hooks", "post-receive"),
		filepath.Join("ruby", "git-hooks", "update"),
	} {
		if err := ioutil.WriteFile(filepath.Join(tmpPath, filePath), nil, 0755); err != nil {
			return err
		}
	}
	config.Config.GitlabShell = config.GitlabShell{
		Dir: filepath.Join(tmpPath, "gitlab-shell"),
	}
	config.Config.Ruby = config.Ruby{
		Dir: filepath.Join(tmpPath, "ruby"),
	}

	config.Config.BinDir = filepath.Join(tmpPath, "bin")

	return nil
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
