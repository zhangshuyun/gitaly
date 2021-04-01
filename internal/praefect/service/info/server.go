package info

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/service"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// AssignmentStore is an interface for getting repository host node assignments.
//
// This duplicates the praefect.AssignmentGetter type as it is not possible to import anything from
// `praefect` to `info` packages due to cyclic dependencies.
type AssignmentStore interface {
	// GetHostAssignments returns the names of the storages assigned to host the repository.
	// The primary node must always be assigned.
	GetHostAssignments(ctx context.Context, virtualStorage, relativePath string) ([]string, error)
	// SetReplicationFactor sets a repository's replication factor and returns the current assignments.
	SetReplicationFactor(ctx context.Context, virtualStorage, relativePath string, replicationFactor int) ([]string, error)
}

// PrimaryGetter is an interface for getting a primary of a repository.
//
// This duplicates the praefect.PrimaryGetter type as it is not possible to import anything from
// `praefect` to `info` packages due to cyclic dependencies.
type PrimaryGetter interface {
	// GetPrimary returns the primary storage for a given repository.
	GetPrimary(ctx context.Context, virtualStorage string, relativePath string) (string, error)
}

// Server is a InfoService server
type Server struct {
	nodeMgr         nodes.Manager
	conf            config.Config
	queue           datastore.ReplicationEventQueue
	rs              datastore.RepositoryStore
	assignmentStore AssignmentStore
	conns           service.Connections
	primaryGetter   PrimaryGetter
}

// NewServer creates a new instance of a grpc InfoServiceServer
func NewServer(
	nodeMgr nodes.Manager,
	conf config.Config,
	queue datastore.ReplicationEventQueue,
	rs datastore.RepositoryStore,
	assignmentStore AssignmentStore,
	conns service.Connections,
	primaryGetter PrimaryGetter,
) gitalypb.PraefectInfoServiceServer {
	return &Server{
		nodeMgr:         nodeMgr,
		conf:            conf,
		queue:           queue,
		rs:              rs,
		assignmentStore: assignmentStore,
		conns:           conns,
		primaryGetter:   primaryGetter,
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
