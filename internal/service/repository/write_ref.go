package repository

import (
	"bytes"
	"fmt"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) WriteRef(ctx context.Context, req *gitalypb.WriteRefRequest) (*gitalypb.WriteRefResponse, error) {
	if err := validateWriteRefRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "WriteRef: %v", err)
	}

	client, err := s.RepositoryServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.WriteRef(clientCtx, req)
}

func validateWriteRefRequest(req *gitalypb.WriteRefRequest) error {
	if err := git.ValidateRevision(req.Ref); err != nil {
		return fmt.Errorf("invalid ref: %v", err)
	}
	if err := git.ValidateRevision(req.Revision); err != nil {
		return fmt.Errorf("invalid revision: %v", err)
	}
	if len(req.OldRevision) > 0 {
		if err := git.ValidateRevision(req.OldRevision); err != nil {
			return fmt.Errorf("invalid OldRevision: %v", err)
		}
	}

	if !bytes.Equal(req.Ref, []byte("HEAD")) && !bytes.HasPrefix(req.Ref, []byte("refs/")) {
		return fmt.Errorf("ref has to be a full reference")
	}
	return nil
}
