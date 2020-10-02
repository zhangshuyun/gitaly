package info

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

// Server is a InfoService server
type Server struct {
	nodeMgr nodes.Manager
	conf    config.Config
	queue   datastore.ReplicationEventQueue
	rs      datastore.RepositoryStore
}

// NewServer creates a new instance of a grpc InfoServiceServer
func NewServer(nodeMgr nodes.Manager, conf config.Config, queue datastore.ReplicationEventQueue, rs datastore.RepositoryStore) gitalypb.PraefectInfoServiceServer {
	return &Server{
		nodeMgr: nodeMgr,
		conf:    conf,
		queue:   queue,
		rs:      rs,
	}
}

func (s *Server) SetAuthoritativeStorage(ctx context.Context, req *gitalypb.SetAuthoritativeStorageRequest) (*gitalypb.SetAuthoritativeStorageResponse, error) {
	storages := s.conf.StorageNames()[req.VirtualStorage]
	if storages == nil {
		return nil, helper.ErrInvalidArgumentf("unknown virtual storage: %q", req.VirtualStorage)
	}

	foundStorage := false
	for i := range storages {
		if storages[i] == req.AuthoritativeStorage {
			foundStorage = true
			break
		}
	}

	if !foundStorage {
		return nil, helper.ErrInvalidArgumentf("unknown authoritative storage: %q", req.AuthoritativeStorage)
	}

	exists, err := s.rs.RepositoryExists(ctx, req.VirtualStorage, req.RelativePath)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, helper.ErrInvalidArgumentf("repository %q does not exist on virtual storage %q", req.RelativePath, req.VirtualStorage)
	}

	if err := s.rs.IncrementGeneration(ctx, req.VirtualStorage, req.RelativePath, req.AuthoritativeStorage, nil); err != nil {
		return nil, helper.ErrInternal(err)
	}

	// Schedule replication jobs to other physical storages to get them consistent with the
	// new authoritative repository.
	for _, storage := range storages {
		if storage == req.AuthoritativeStorage {
			continue
		}

		if _, err := s.queue.Enqueue(ctx, datastore.ReplicationEvent{
			Job: datastore.ReplicationJob{
				Change:            datastore.UpdateRepo,
				VirtualStorage:    req.VirtualStorage,
				RelativePath:      req.RelativePath,
				SourceNodeStorage: req.AuthoritativeStorage,
				TargetNodeStorage: storage,
			},
		}); err != nil {
			return nil, helper.ErrInternal(err)
		}
	}

	return &gitalypb.SetAuthoritativeStorageResponse{}, nil
}
