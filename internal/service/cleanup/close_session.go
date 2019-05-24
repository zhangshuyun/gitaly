package cleanup

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
)

func (s *server) CloseSession(ctx context.Context, in *gitalypb.CloseSessionRequest) (*gitalypb.CloseSessionResponse, error) {
	if err := validateCloseSessionRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	catfile.EvictCacheBySessionID(in.GetSessionId())

	return &gitalypb.CloseSessionResponse{}, nil
}

func validateCloseSessionRequest(in *gitalypb.CloseSessionRequest) error {
	if in.GetSessionId() == "" {
		return errors.New("session_id is required")
	}

	return nil
}
