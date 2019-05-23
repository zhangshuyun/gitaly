package diskcache

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/etcd-io/bbolt"
	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
)

type CacheDB struct {
	db     *bbolt.DB
	dbPath string
	dbLock sync.RWMutex // coordinates reset event
}

var cacheDBOpts = &bbolt.Options{
	Timeout: time.Second,
}

func CreateDB(dbPath string) (*CacheDB, error) {
	db, err := bbolt.Open(dbPath, 0644, cacheDBOpts)
	if err != nil {
		return nil, err
	}

	return &CacheDB{
		db:     db,
		dbPath: dbPath,
	}, nil
}

var (
	ErrNamespaceNotFound = errors.New("namespace not found")
	ErrKeyNotFound       = errors.New("key not found within namespace")
)

// GetStream will fetch the cached stream for a request within the provided
// repository namespace. It is the responsibility of the caller to close the
// stream when done.
func (cdb *CacheDB) GetStream(repo *gitalypb.Repository, req proto.Message) (io.ReadCloser, error) {
	cdb.dbLock.RLock()
	defer cdb.dbLock.RUnlock()

	namespace, key, err := namespaceKey(repo, req)
	if err != nil {
		return nil, err
	}

	r, w := io.Pipe()
	ready := make(chan struct{})
	errQ := make(chan error, 1)

	go func() {
		var err error
		defer w.CloseWithError(err)

		err = cdb.db.View(func(tx *bbolt.Tx) error {
			namespaceB := tx.Bucket(namespace)
			if namespaceB == nil {
				return ErrNamespaceNotFound
			}

			keyB := namespaceB.Bucket(key)
			if keyB == nil {
				return ErrKeyNotFound
			}

			ready <- struct{}{} // cache hit!

			return keyB.ForEach(func(k []byte, v []byte) error {
				// boltdb returned data is only valid during a transaction,
				// thus copying is necessary to ensure data is safe. See
				// project docs for more info:
				// https://godoc.org/github.com/etcd-io/bbolt#hdr-Caveats
				v2 := make([]byte, len(v))
				copy(v2, v)
				_, err := w.Write(v2)
				return err
			})

			return nil
		})
		errQ <- err // may never be received, thus buffer size of 1
	}()

	select {
	case err := <-errQ:
		// cache miss or other error
		return nil, err
	case <-ready:
		// cache hit! continue...
	}

	return r, nil
}

func (cdb *CacheDB) PutStream(repo *gitalypb.Repository, req proto.Message, src io.Reader) error {
	cdb.dbLock.RLock()
	defer cdb.dbLock.RUnlock()

	namespace, key, err := namespaceKey(repo, req)
	if err != nil {
		return err
	}

	return cdb.db.Update(func(tx *bbolt.Tx) error {
		namespaceB, err := tx.CreateBucketIfNotExists(namespace)
		if err != nil {
			return err
		}

		keyB, err := namespaceB.CreateBucket(key)
		switch err {
		case nil:
			break
		case bbolt.ErrBucketExists:
			if err := namespaceB.DeleteBucket(key); err != nil {
				return err
			}
			// try again...
			keyB, err = namespaceB.CreateBucket(key)
			if err != nil {
				return err
			}
		default:
			return err
		}

		var seq uint64
		rdr := bufio.NewReader(src)

		for {
			b := make([]byte, os.Getpagesize())

			n, err := rdr.Read(b)
			switch err {
			case nil:
				break
			case io.EOF:
				return nil
			default:
				return err
			}

			seq, err = keyB.NextSequence()
			if err != nil {
				return err
			}

			if err := keyB.Put(itob(seq), b[0:n]); err != nil {
				return err
			}

		}
	})
}

// InvalidateRepo will delete the repo's namespace. This is useful when the repo
// has been modified and the cache can no longer be trusted.
func (cdb *CacheDB) InvalidateRepo(repo *gitalypb.Repository) error {
	cdb.dbLock.RLock()
	defer cdb.dbLock.RUnlock()

	namespace, err := protoSHA256(repo)
	if err != nil {
		return err
	}

	return cdb.db.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket([]byte(namespace))
	})
}

// itob returns an 8-byte big endian representation of v.
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// Reset will wait for all pending operations to complete and then destroy and
// recreate the database.
func (cdb *CacheDB) Reset() error {
	cdb.dbLock.Lock()
	defer cdb.dbLock.Unlock()

	if err := cdb.db.Close(); err != nil {
		return err
	}

	if err := os.Remove(cdb.dbPath); err != nil {
		return err
	}

	db, err := bbolt.Open(cdb.dbPath, 0644, cacheDBOpts)
	if err != nil {
		return err
	}

	cdb.db = db
	return nil
}

func namespaceKey(repo *gitalypb.Repository, req proto.Message) (namespace []byte, key []byte, err error) {
	namespace, err = protoSHA256(repo)
	if err != nil {
		return
	}

	key, err = protoSHA256(req)
	if err != nil {
		return
	}

	return
}

func protoSHA256(msg proto.Message) ([]byte, error) {
	pb, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	sum := sha256.Sum256(pb)
	return sum[:], nil
}
