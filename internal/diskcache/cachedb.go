package diskcache

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"time"

	"github.com/etcd-io/bbolt"
)

type CacheDB struct {
	db     *bbolt.DB
	dbPath string
}

func CreateDB(dbPath string) (*CacheDB, error) {
	options := &bbolt.Options{
		Timeout: time.Second,
	}

	db, err := bbolt.Open(dbPath, 0644, options)
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

// GetStream will fetch the key value within the provided namespace. It is the
// responsibility of the caller to close the stream when done.
func (cdb *CacheDB) GetStream(namespace, key string) (io.ReadCloser, error) {
	r, w := io.Pipe()

	go func() {
		var err error
		defer w.CloseWithError(err)

		err = cdb.db.View(func(tx *bbolt.Tx) error {
			namespaceB := tx.Bucket([]byte(namespace))
			if namespaceB == nil {
				return ErrNamespaceNotFound
			}

			keyB := namespaceB.Bucket([]byte(key))
			if keyB == nil {
				return ErrKeyNotFound
			}

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
	}()

	return r, nil
}

func (cdb *CacheDB) PutStream(namespace, key string, src io.Reader) error {
	return cdb.db.Update(func(tx *bbolt.Tx) error {
		namespaceB, err := tx.CreateBucketIfNotExists([]byte(namespace))
		if err != nil {
			return err
		}

		keyB, err := namespaceB.CreateBucket([]byte(key))
		switch err {
		case nil:
			break
		case bbolt.ErrBucketExists:
			if err := namespaceB.DeleteBucket([]byte(key)); err != nil {
				return err
			}
			// try again...
			keyB, err = namespaceB.CreateBucket([]byte(key))
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

// itob returns an 8-byte big endian representation of v.
func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func (cdb *CacheDB) Destroy() error {
	if err := cdb.db.Close(); err != nil {
		return err
	}

	return os.Remove(cdb.dbPath)
}
