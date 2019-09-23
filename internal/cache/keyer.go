package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/safe"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

var (
	// ErrMissingLeaseFile indicates a lease file does not exist on the
	// filesystem that the lease ender expected to be there
	ErrMissingLeaseFile = errors.New("lease file unexpectedly missing")
	// ErrInvalidUUID indicates an internal error with generating a UUID
	ErrInvalidUUID = errors.New("unable to generate valid UUID")
	// ErrCtxMethodMissing indicates the provided context does not contain the
	// expected information about the current gRPC method
	ErrCtxMethodMissing = errors.New("context does not contain gRPC method name")
	// ErrPendingExists indicates that there is a critical zone for the current
	// repository in the pending transition
	ErrPendingExists = errors.New("one or more cache generations are pending transition for the current repository")
)

// Keyer abstracts how to obtain a unique file path key for a request at a
// specific generation of the cache. The key path will magically update as new
// critical sections are declared. An error will be returned if the repo's cache
// has any open critical sections.
type Keyer interface {
	// KeyPath will return a key filepath for the provided request. If an error
	// is returned, the cache should not be used.
	KeyPath(context.Context, *gitalypb.Repository, proto.Message) (string, error)
}

// LeaseKeyer will try to return a key path for the current generation of
// the repo's cache. It uses a strategy that avoids file locks in favor of
// atomically created/renamed files. Read more about LeaseKeyer's design:
// https://gitlab.com/gitlab-org/gitaly/issues/1745
type LeaseKeyer struct{}

type lease struct {
	pendingPath string
	repo        *gitalypb.Repository
}

// EndLease will end the lease by removing the pending lease file and updating
// the key file with the current lease ID.
func (l lease) EndLease(ctx context.Context) error {
	_, err := updateLatest(l.repo)
	if err != nil {
		return err
	}

	if err := os.Remove(l.pendingPath); err != nil {
		if os.IsNotExist(err) {
			return countErr(ErrMissingLeaseFile)
		}
		return err
	}

	return nil
}

func updateLatest(repo *gitalypb.Repository) (string, error) {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return "", err
	}

	lPath := latestPath(repoPath)
	if err := os.MkdirAll(filepath.Dir(lPath), 0755); err != nil {
		return "", err
	}

	latest, err := safe.CreateFileWriter(lPath)
	if err != nil {
		return "", err
	}
	defer latest.Close()

	nextGenID := uuid.New().String()
	if nextGenID == "" {
		return "", ErrInvalidUUID
	}

	if _, err = latest.Write([]byte(nextGenID)); err != nil {
		return "", err
	}

	if err := latest.Commit(); err != nil {
		return "", err
	}

	return nextGenID, nil
}

// LeaseEnder allows the caller to indicate when a lease is no longer needed
type LeaseEnder interface {
	EndLease(context.Context) error
}

// StartLease will mark the repository as being in an indeterministic state.
// This is typically used when modifying the repo, since the cache is not
// stable until after the modification is complete. A lease object will be
// returned that allows the caller to signal the end of the lease.
func (LeaseKeyer) StartLease(repo *gitalypb.Repository) (LeaseEnder, error) {
	pendingPath, err := newPendingLease(repo)
	if err != nil {
		return lease{}, err
	}

	return lease{
		pendingPath: pendingPath,
		repo:        repo,
	}, nil
}

// staleAge is how old we consider a pending file to be stale before removal
const staleAge = time.Hour

// KeyPath will attempt to return the unique keypath for a request in the
// specified repo for the current generation. The context must contain the gRPC
// method in its values.
func (LeaseKeyer) KeyPath(ctx context.Context, repo *gitalypb.Repository, req proto.Message) (string, error) {
	pending, err := currentLeases(repo)
	if err != nil {
		return "", err
	}

	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return "", err
	}

	pDir := pendingDir(repoPath)

	anyValidPending := false
	for _, p := range pending {
		if time.Since(p.ModTime()) > staleAge {
			pPath := filepath.Join(pDir, p.Name())
			if err := os.Remove(pPath); err != nil && !os.IsNotExist(err) {
				return "", err
			}
			continue
		}
		anyValidPending = true
	}

	if anyValidPending {
		return "", countErr(ErrPendingExists)
	}

	genID, err := currentGenID(repo)
	if err != nil {
		return "", err
	}

	key, err := compositeKeyHashHex(ctx, genID, req)
	if err != nil {
		return "", err
	}

	cDir, err := cacheDir(repo)
	if err != nil {
		return "", err
	}

	return radixPath(cDir, key)
}

// radixPath is the same directory structure scheme used by git. This scheme
// allows for the objects to be randomly distributed across folders based on
// the first 2 hex chars of the key (i.e. 256 possible top level folders).
func radixPath(root, key string) (string, error) {
	return filepath.Join(root, key[0:2], key[2:]), nil
}

func newPendingLease(repo *gitalypb.Repository) (string, error) {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return "", err
	}

	pDir := pendingDir(repoPath)
	if err := os.MkdirAll(pDir, 0755); err != nil {
		return "", err
	}

	f, err := ioutil.TempFile(pDir, "")
	if err != nil {
		return "", err
	}

	if err := f.Close(); err != nil {
		return "", err
	}

	return f.Name(), nil
}

// cacheDir is $STORAGE/+gitaly/cache
func cacheDir(repo *gitalypb.Repository) (string, error) {
	storage, ok := config.Config.Storage(repo.StorageName)
	if !ok {
		return "", fmt.Errorf("storage not found for %v", repo)
	}

	return tempdir.CacheDir(storage), nil
}

func currentLeases(repo *gitalypb.Repository) ([]os.FileInfo, error) {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}

	pendings, err := ioutil.ReadDir(pendingDir(repoPath))
	switch {
	case os.IsNotExist(err):
		// pending files subdir don't exist yet, that's okay
		break
	case err == nil:
		break
	default:
		return nil, err
	}

	return pendings, nil
}

func currentGenID(repo *gitalypb.Repository) (string, error) {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return "", err
	}

	latestBytes, err := ioutil.ReadFile(latestPath(repoPath))
	switch {
	case os.IsNotExist(err):
		// latest file doesn't exist, so create one
		return updateLatest(repo)
	case err == nil:
		return string(latestBytes), nil
	default:
		return "", err
	}
}

func stateDir(repoDir string) string   { return filepath.Join(repoDir, "state") }
func pendingDir(repoDir string) string { return filepath.Join(stateDir(repoDir), "pending") }
func latestPath(repoDir string) string { return filepath.Join(stateDir(repoDir), "latest") }

// compositeKeyHashHex returns a hex encoded string that is a SHA256 hash sum of
// the composite key made up of the following properties: Gitaly version, gRPC
// method, repo cache current generation ID, protobuf request
func compositeKeyHashHex(ctx context.Context, genID string, req proto.Message) (string, error) {
	method, ok := grpc.Method(ctx)
	if !ok {
		return "", ErrCtxMethodMissing
	}

	reqSum, err := proto.Marshal(req)
	if err != nil {
		return "", err
	}

	h := sha256.New()

	for _, i := range []string{
		version.GetVersion(),
		method,
		genID,
		string(reqSum),
	} {
		_, err := h.Write(prefixLen(i))
		if err != nil {
			return "", err
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// prefixLen reduces the risk of collisions due to different combinations of
// concatenated strings producing the same content.
// e.g. f+oobar and foo+bar concatenate to the same thing: foobar
func prefixLen(s string) []byte {
	return []byte(fmt.Sprintf("%08x%s", len(s), s))
}
