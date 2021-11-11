package repocleaner

import (
	"context"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// Walker allows iterating over the repositories of the gitaly storage.
type Walker struct {
	conns     praefect.Connections
	batchSize int
}

// NewWalker returns a new *Walker instance.
func NewWalker(conns praefect.Connections, batchSize int) *Walker {
	return &Walker{conns: conns, batchSize: batchSize}
}

// ExecOnRepositories runs through all the repositories on a Gitaly storage and executes the provided action.
// The processing is done in batches to reduce cost of operations.
func (wr *Walker) ExecOnRepositories(ctx context.Context, virtualStorage, storage string, action func([]datastore.RepositoryClusterPath) error) error {
	gclient, err := wr.getInternalGitalyClient(virtualStorage, storage)
	if err != nil {
		return fmt.Errorf("setup gitaly client: %w", err)
	}

	resp, err := gclient.WalkRepos(ctx, &gitalypb.WalkReposRequest{StorageName: storage})
	if err != nil {
		return fmt.Errorf("unable to walk repos: %w", err)
	}

	batch := make([]datastore.RepositoryClusterPath, 0, wr.batchSize)
	for {
		res, err := resp.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return fmt.Errorf("failure on walking repos: %w", err)
			}
			break
		}

		batch = append(batch, datastore.RepositoryClusterPath{
			ClusterPath: datastore.ClusterPath{
				VirtualStorage: virtualStorage,
				Storage:        storage,
			},
			RelativePath: res.RelativePath,
		})

		if len(batch) == cap(batch) {
			if err := action(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := action(batch); err != nil {
			return err
		}
	}
	return nil
}

func (wr *Walker) getInternalGitalyClient(virtualStorage, storage string) (gitalypb.InternalGitalyClient, error) {
	conn, found := wr.conns[virtualStorage][storage]
	if !found {
		return nil, fmt.Errorf("no connection to the gitaly node %q/%q", virtualStorage, storage)
	}
	return gitalypb.NewInternalGitalyClient(conn), nil
}
