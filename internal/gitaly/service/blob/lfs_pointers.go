package blob

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v13/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// These limits are used as a heuristic to ignore files which can't be LFS
	// pointers. The format of these is described in
	// https://github.com/git-lfs/git-lfs/blob/master/docs/spec.md#the-pointer

	// LfsPointerMinSize is the minimum size for an lfs pointer text blob
	LfsPointerMinSize = 120
	// LfsPointerMaxSize is the maximum size for an lfs pointer text blob
	LfsPointerMaxSize = 200
)

type getLFSPointerByRevisionRequest interface {
	GetRepository() *gitalypb.Repository
	GetRevision() []byte
}

func (s *server) GetLFSPointers(req *gitalypb.GetLFSPointersRequest, stream gitalypb.BlobService_GetLFSPointersServer) error {
	ctx := stream.Context()

	if err := validateGetLFSPointersRequest(req); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetLFSPointers: %v", err)
	}

	client, err := s.ruby.BlobServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetLFSPointers(clientCtx, req)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
}

func validateGetLFSPointersRequest(req *gitalypb.GetLFSPointersRequest) error {
	if req.GetRepository() == nil {
		return errors.ErrEmptyRepository
	}

	if len(req.GetBlobIds()) == 0 {
		return fmt.Errorf("empty BlobIds")
	}

	return nil
}

func (s *server) GetNewLFSPointers(in *gitalypb.GetNewLFSPointersRequest, stream gitalypb.BlobService_GetNewLFSPointersServer) error {
	ctx := stream.Context()

	if err := validateGetLfsPointersByRevisionRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetNewLFSPointers: %v", err)
	}

	client, err := s.ruby.BlobServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetNewLFSPointers(clientCtx, in)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
}

func (s *server) GetAllLFSPointers(in *gitalypb.GetAllLFSPointersRequest, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	ctx := stream.Context()

	if in.GetRepository() == nil {
		return helper.ErrInvalidArgument(fmt.Errorf("empty Repository"))
	}

	client, err := s.ruby.BlobServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetAllLFSPointers(clientCtx, in)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
}

func validateGetLfsPointersByRevisionRequest(in getLFSPointerByRevisionRequest) error {
	if in.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	return git.ValidateRevision(in.GetRevision())
}
