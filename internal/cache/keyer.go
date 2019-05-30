package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dchest/safefile"
	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"google.golang.org/grpc"
)

const (
	// how old a cached entry is before the cleanup worker removes it
	staleTimeout        = time.Hour
	cacheManagerVersion = 1
	genFileSuffix       = ".lock"
)

// CacheKeyer abstracts how to obtain a unique file path key for a request at a
// specific generation of the cache. The key path will magically update as new
// critical sections are declared.
type CacheKeyer interface {
	// Key will return a key filepath for the provided request. If an error is
	// returned, the cache should not be used.
	CacheKeyPath(context.Context, *gitalypb.Repository, proto.Message) (string, error)
}

// NaiveKeyer will try to return a key path for the current generation of
// the repo's cache. It is possible for it to return stale key paths that have
// been invalidated, but these invalid entries should only be returned within
// a narrow time window (see staleTimeout)
type NaiveKeyer struct{}

// StartCriticalSection is a no-op for the naive strategy
// TODO: determine a safe way to signal the start of a critical section
func (NaiveKeyer) StartCriticalSection(_ *gitalypb.Repository) error {
	return nil
}

// EndCriticalSection will try to end a critical section. If a race condition
// occurs where the desired generation file already exists, an error will be
// returned.
func (NaiveKeyer) EndCriticalSection(repo *gitalypb.Repository) error {
	cDir, err := cacheDir(repo)
	if err != nil {
		return err
	}

	curGen, err := currentGeneration(cDir)
	if err != nil {
		return err
	}

	genPath := filepath.Join(cDir, fmt.Sprintf("%x%s", curGen, genFileSuffix))
	log.Print(genPath)

	genFile, err := safefile.Create(genPath, lockFilePerms)
	if err != nil {
		return err
	}

	if err := genFile.Commit(); err != nil {
		return err
	}

	return nil
}

var ErrCtxMethodMissing = errors.New("context does not contain gRPC method name")

// KeyPath will attempt to return the unique keypath for a request in the
// specified repo for the current generation. The context must contain the gRPC
// method in its values.
func (NaiveKeyer) CacheKeyPath(ctx context.Context, repo *gitalypb.Repository, req proto.Message) (string, error) {
	cDir, err := cacheDir(repo)
	if err != nil {
		return "", err
	}

	kName, err := keyName(ctx, cDir, req)
	if err != nil {
		return "", err
	}

	return filepath.Join(cDir, kName), nil
}

// cacheDir is $STORAGE/$CACHE_PREFIX/$CACHE_VERSION/$REPO_RELPATH
func cacheDir(repo *gitalypb.Repository) (string, error) {
	storagePath, err := helper.GetStorageByName(repo.StorageName)
	if err != nil {
		return "", err
	}

	absPath := filepath.Join(
		storagePath,
		tempdir.CachePrefix,
		fmt.Sprintf("v%d", cacheManagerVersion),
		repo.RelativePath,
	)

	return absPath, nil
}

// currentGeneration will attempt to retrieve the latest generation lock
// filename available in the cacheDir
func currentGeneration(cacheDir string) (uint64, error) {
	genFiles, err := filepath.Glob(filepath.Join(cacheDir, "*"+genFileSuffix))
	if err != nil {
		return 0, err
	}

	if len(genFiles) < 1 {
		// no generation files could indicate this is the first one
		return 1, nil
	}

	sort.Strings(genFiles)
	latestGenName := filepath.Base(genFiles[len(genFiles)-1])
	latestGenHex := strings.TrimSuffix(latestGenName, genFileSuffix)
	gen, err := strconv.ParseUint(latestGenHex, 16, 64)
	if err != nil {
		return 0, err
	}

	return gen, nil
}

// keyName returns a filename that is a SHA256 hash sum of the composite key
// made up of the following properties: Gitaly version, gRPC method, repo cache
// generation, protobuf request
func keyName(ctx context.Context, cDir string, req proto.Message) (string, error) {
	method, ok := grpc.Method(ctx)
	if !ok {
		return "", ErrCtxMethodMissing
	}

	reqSum, err := proto.Marshal(req)
	if err != nil {
		return "", err
	}

	curGen, err := currentGeneration(cDir)
	if err != nil {
		return "", err
	}

	h := sha256.New()

	for _, i := range [][]byte{
		[]byte(version.GetVersion()),
		[]byte(method),
		[]byte(strconv.FormatUint(curGen, 16)),
		reqSum,
	} {
		_, err := h.Write(i)
		if err != nil {
			return "", err
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
