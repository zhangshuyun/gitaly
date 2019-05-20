// Package global is a convenient place to maintain global constructs and
// singletons used throughout Gitaly.
package global

import (
	"errors"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/diskcache"
)

const diskcacheDBName = "diskcache.bboltdb"

var diskcacheDB *diskcache.CacheDB

var (
	ErrNoStorages = errors.New("no configured storages to persist cache")
)

func DiskcacheDB() (*diskcache.CacheDB, error) {
	if diskcacheDB != nil {
		return diskcacheDB, nil
	}

	if len(config.Config.Storages) < 1 {
		return nil, ErrNoStorages
	}

	dbPath := filepath.Join(config.Config.Storages[0].Path, diskcacheDBName)
	logrus.Infof("Creating global disk cache DB at %s", dbPath)
	dcdb, err := diskcache.CreateDB(dbPath)
	if err != nil {
		return nil, err
	}

	diskcacheDB = dcdb

	return diskcacheDB, nil
}
