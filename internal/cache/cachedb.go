package cache

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"

	"github.com/dchest/safefile"
	"github.com/gofrs/flock"
	"github.com/golang/protobuf/proto"
)

const (
	lockFilePerms = 0640
	lockFileName  = "streamdb.lock"
)

// StreamDB stores and retrieves byte streams for repository related RPCs
type StreamDB struct {
	root string
	lock *flock.Flock
	ck   CacheKeyer
}

// OpenRepoDB will open the stream database at the specified file path.
func OpenStreamDB(root string, ck CacheKeyer) (*StreamDB, error) {
	return &StreamDB{
		root: root,
		ck:   ck,
	}, nil
}

// ErrRepoNotFound indicates the repo namespace doesn't exist in the cache
// ErrReqNotFound indicates the request does not exist within the repo digest
var (
	ErrReqNotFound = errors.New("request digest not found within repo namespace")
)

// GetStream will fetch the cached stream for a request. It is the
// responsibility of the caller to close the stream when done.
func (sdb *StreamDB) GetStream(ctx context.Context, repo *gitalypb.Repository, req proto.Message) (io.ReadCloser, error) {
	respPath, err := sdb.ck.CacheKeyPath(ctx, repo, req)
	if err != nil {
		return nil, err
	}
	log.Print(respPath)

	respF, err := os.Open(respPath)
	if err != nil {
		return nil, err
	}

	return respF, nil
}

// PutStream will store a stream in a repo-namespace keyed by the digest of the
// request protobuf message.
func (sdb *StreamDB) PutStream(ctx context.Context, repo *gitalypb.Repository, req proto.Message, src io.Reader) error {
	reqPath, err := sdb.ck.CacheKeyPath(ctx, repo, req)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(reqPath), 0755); err != nil {
		return err
	}

	sf, err := safefile.Create(reqPath, lockFilePerms)
	if err != nil {
		return err
	}
	defer sf.Close()

	if _, err := io.Copy(sf, src); err != nil {
		return err
	}

	if err := sf.Commit(); err != nil {
		return err
	}

	return nil
}

// ProtoSHA256 returns a SHA256 digest of a serialized protobuf message
func ProtoSHA256(msg proto.Message) ([]byte, error) {
	pb, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	sum := sha256.Sum256(pb)
	return sum[:], nil
}
