package streamcache

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
)

var (
	fileRemoveCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_streamcache_filestore_removed_total",
			Help: "Number of files removed from streamcache via file walks",
		},
		[]string{"dir"},
	)
	diskUsageGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gitaly_streamcache_filestore_disk_usage_bytes",
			Help: "Disk usage per filestore",
		},
		[]string{"dir"},
	)
)

// Filestore creates temporary files in dir. These files get deleted once
// they are older (by mtime) than maxAge via a goroutine that does
// periodic file walks. To make these file walks efficient, we spread the
// temporary files across a balanced directory tree in subdirectories
// aa/my-temp-file where aa is a hexadecimal number. This means
// we eventually create exactly 256 subdirectories.
//
// Each instance of filestore carries a random token which is used as a
// prefix in the tempfiles it creates. This ensures that it is safe to
// have multiple instances of filestore use the same directory on disk;
// their tempfiles will not collide. There is one caveat: if multiple
// filestores share the same directory but have different maximum ages,
// then lowest maximum age becomes the effective maximum age for all of
// them.
type filestore struct {
	dir    string
	maxAge time.Duration

	m       sync.Mutex
	id      []byte
	counter uint64
	stop    chan struct{}
}

func newFilestore(dir string, maxAge time.Duration, sleep func(time.Duration), logger logrus.FieldLogger) *filestore {
	fs := &filestore{
		dir:    dir,
		maxAge: maxAge,
		stop:   make(chan struct{}),
	}

	dontpanic.GoForever(1*time.Minute, func() {
		sleepLoop(fs.stop, fs.maxAge, sleep, func() {
			diskUsageGauge.WithLabelValues(fs.dir).Set(fs.diskUsage())

			if err := fs.cleanWalk(time.Now().Add(-fs.maxAge)); err != nil {
				logger.WithError(err).Error("streamcache filestore cleanup")
			}
		})
	})

	return fs
}

type namedWriteCloser interface {
	Name() string
	io.WriteCloser
}

// Create creates a new tempfile. It does not use ioutil.TempFile because
// the documentation of TempFile makes no promises about reusing tempfile
// names after a file has been deleted. By using a very large (uint64)
// counter, Create makes it clear / explicit how unlikely reuse is.
func (fs *filestore) Create() (namedWriteCloser, error) {
	if err := fs.ensureCacheID(); err != nil {
		return nil, err
	}

	fileID := fs.nextFileID()

	name := fmt.Sprintf("%x-%d",
		// fs.id ensures uniqueness among other *filestore instances
		fs.id,
		// fileID ensures uniqueness (modulo roll-over) among other files
		// created by this *filestore instance
		fileID,
	)

	path := filepath.Join(fs.dir, fmt.Sprintf("%02x", uint8(fileID)), name)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("Create: mkdir: %w", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, fmt.Errorf("Create: %w", err)
	}

	return f, nil
}

func (fs *filestore) ensureCacheID() error {
	fs.m.Lock()
	defer fs.m.Unlock()

	if len(fs.id) == 0 {
		buf := make([]byte, 10)
		if _, err := io.ReadFull(rand.Reader, buf); err != nil {
			return err
		}
		fs.id = buf
	}

	return nil
}

func (fs *filestore) nextFileID() uint64 {
	fs.m.Lock()
	defer fs.m.Unlock()
	fs.counter++
	return fs.counter
}

func (fs *filestore) Stop() {
	fs.m.Lock()
	defer fs.m.Unlock()

	select {
	case <-fs.stop:
	default:
		close(fs.stop)
	}
}

// cleanWalk removes files but not directories. This is to avoid races
// when a directory looks empty but another goroutine is about to create
// a new file in it with fs.Create(). Because the number of directories
// is bounded by the directory scheme to 256, there is no need to remove
// the directories anyway.
func (fs *filestore) cleanWalk(cutoff time.Time) error {
	// If a server reset has left some directories in a bad state, this will
	// fix it.
	if err := housekeeping.FixDirectoryPermissions(context.Background(), fs.dir); err != nil {
		return err
	}

	return filepath.Walk(fs.dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && info.ModTime().Before(cutoff) {
			err = os.Remove(path)
			fileRemoveCounter.WithLabelValues(fs.dir).Inc()
		}

		if os.IsNotExist(err) {
			err = nil
		}

		return err
	})
}

func (fs *filestore) diskUsage() float64 {
	var total float64
	_ = filepath.Walk(fs.dir, func(_ string, info os.FileInfo, _ error) error {
		if info != nil {
			total += float64(info.Size())
		}
		return nil
	})
	return total
}
