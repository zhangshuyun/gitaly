package info

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// ReplicationFactorSetter sets a repository's replication factor
type ReplicationFactorSetter interface {
	// SetReplicationFactor assigns or unassigns a repository's host nodes until the desired replication factor is met.
	// Please see the protobuf documentation of the method for details.
	SetReplicationFactor(ctx context.Context, virtualStorage, relativePath string, replicationFactor int) ([]string, error)
}

func (s *Server) SetReplicationFactor(ctx context.Context, req *gitalypb.SetReplicationFactorRequest) (*gitalypb.SetReplicationFactorResponse, error) {
	resp, err := s.setReplicationFactor(ctx, req)
	if err != nil {
		var invalidArg datastore.InvalidArgumentError
		if errors.As(err, &invalidArg) {
			return nil, helper.ErrInvalidArgument(err)
		}

		return nil, helper.ErrInternal(err)
	}

	return resp, nil
}

func (s *Server) setReplicationFactor(ctx context.Context, req *gitalypb.SetReplicationFactorRequest) (*gitalypb.SetReplicationFactorResponse, error) {
	storages, err := s.rfs.SetReplicationFactor(ctx, req.VirtualStorage, req.RelativePath, int(req.ReplicationFactor))
	if err != nil {
		return nil, fmt.Errorf("set replication factor: %w", err)
	}

	return &gitalypb.SetReplicationFactorResponse{Storages: storages}, nil
}
