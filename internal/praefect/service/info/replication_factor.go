package info

import (
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

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
	storages, err := s.assignmentStore.SetReplicationFactor(ctx, req.VirtualStorage, req.RelativePath, int(req.ReplicationFactor))
	if err != nil {
		return nil, fmt.Errorf("set replication factor: %w", err)
	}

	return &gitalypb.SetReplicationFactorResponse{Storages: storages}, nil
}
