// Package cache supplies background workers for periodically cleaning the
// cache folder on all storages listed in the config file. Upon configuration
// validation, one worker will be started for each storage. The worker will
// walk the cache directory tree and remove any files older than one hour. The
// worker will walk the cache directory every ten minutes.
package cache

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
)

func logWalkErr(err error, path, msg string) {
	countWalkError()
	log.Default().
		WithField("path", path).
		WithError(err).
		Warn(msg)
}

func cleanWalk(path string) error {
	defer time.Sleep(100 * time.Microsecond) // relieve pressure

	countWalkCheck()
	entries, err := ioutil.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		logWalkErr(err, path, "unable to stat directory")
		return err
	}

	for _, e := range entries {
		ePath := filepath.Join(path, e.Name())

		if e.IsDir() {
			if err := cleanWalk(ePath); err != nil {
				return err
			}
			continue
		}

		countWalkCheck()
		if time.Since(e.ModTime()) < staleAge {
			continue // still fresh
		}

		// file is stale
		if err := os.Remove(ePath); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			logWalkErr(err, ePath, "unable to remove file")
			return err
		}
		countWalkRemoval()
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		logWalkErr(err, path, "unable to stat directory after walk")
		return err
	}

	if len(files) == 0 {
		countEmptyDir()
		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			logWalkErr(err, path, "unable to remove empty directory")
			return err
		}
		countEmptyDirRemoval()
		countWalkRemoval()
	}

	return nil
}

const cleanWalkFrequency = 10 * time.Minute

func walkLoop(walkPath string) {
	logger := logrus.WithField("path", walkPath)
	logger.Infof("Starting file walker for %s", walkPath)

	walkTick := time.NewTicker(cleanWalkFrequency)
	dontpanic.GoForever(time.Minute, func() {
		if err := cleanWalk(walkPath); err != nil {
			logger.Error(err)
		}
		<-walkTick.C
	})
}

func (c *Cache) startCleanWalker(storagePath string) {
	if c.cacheConfig.disableWalker {
		return
	}

	walkLoop(tempdir.AppendCacheDir(storagePath))
	walkLoop(tempdir.AppendStateDir(storagePath))
}

// moveAndClear will move the cache to the storage location's
// temporary folder, and then remove its contents asynchronously
func (c *Cache) moveAndClear(storagePath string) error {
	if c.cacheConfig.disableMoveAndClear {
		return nil
	}

	logger := logrus.WithField("path", storagePath)
	logger.Info("clearing disk cache object folder")

	tempPath := tempdir.AppendTempDir(storagePath)
	if err := os.MkdirAll(tempPath, 0755); err != nil {
		return err
	}

	tmpDir, err := ioutil.TempDir(tempPath, "diskcache")
	if err != nil {
		return err
	}

	defer func() {
		dontpanic.Go(func() {
			start := time.Now()
			if err := os.RemoveAll(tmpDir); err != nil {
				logger.Errorf("unable to remove disk cache objects: %q", err)
				return
			}

			logger.Infof("cleared all cache object files in %s after %s", tmpDir, time.Since(start))
		})
	}()

	logger.Infof("moving disk cache object folder to %s", tmpDir)
	cachePath := tempdir.AppendCacheDir(storagePath)
	if err := os.Rename(cachePath, filepath.Join(tmpDir, "moved")); err != nil {
		if os.IsNotExist(err) {
			logger.Info("disk cache object folder doesn't exist, no need to remove")
			return nil
		}

		return err
	}

	return nil
}

// StartWalkers starts the cache walker Goroutines. Initially, this function will try to clean up
// any preexisting cache directories.
func (c *Cache) StartWalkers() error {
	pathSet := map[string]struct{}{}
	for _, storage := range c.storages {
		pathSet[storage.Path] = struct{}{}
	}

	for sPath := range pathSet {
		if err := c.moveAndClear(sPath); err != nil {
			return err
		}
		c.startCleanWalker(sPath)
	}

	return nil
}
