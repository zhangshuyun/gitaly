package blob

import (
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) GetBlob(in *gitalypb.GetBlobRequest, stream gitalypb.BlobService_GetBlobServer) error {
	ctx := stream.Context()

	if err := validateRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetBlob: %v", err)
	}

	c, err := catfile.New(stream.Context(), s.gitCmdFactory, in.Repository)
	if err != nil {
		return status.Errorf(codes.Internal, "GetBlob: %v", err)
	}

	objectInfo, err := c.Info(ctx, git.Revision(in.Oid))
	if err != nil && !catfile.IsNotFound(err) {
		return status.Errorf(codes.Internal, "GetBlob: %v", err)
	}
	if catfile.IsNotFound(err) || objectInfo.Type != "blob" {
		return helper.DecorateError(codes.Unavailable, stream.Send(&gitalypb.GetBlobResponse{}))
	}

	readLimit := objectInfo.Size
	if in.Limit >= 0 && in.Limit < readLimit {
		readLimit = in.Limit
	}
	firstMessage := &gitalypb.GetBlobResponse{
		Size: objectInfo.Size,
		Oid:  objectInfo.Oid,
	}

	if readLimit == 0 {
		return helper.DecorateError(codes.Unavailable, stream.Send(firstMessage))
	}

	blobObj, err := c.Blob(ctx, git.Revision(objectInfo.Oid))
	if err != nil {
		return status.Errorf(codes.Internal, "GetBlob: %v", err)
	}

	sw := streamio.NewWriter(func(p []byte) error {
		msg := &gitalypb.GetBlobResponse{}
		if firstMessage != nil {
			msg = firstMessage
			firstMessage = nil
		}
		msg.Data = p
		return stream.Send(msg)
	})

	_, err = io.CopyN(sw, blobObj.Reader, readLimit)
	if err != nil {
		return status.Errorf(codes.Unavailable, "GetBlob: send: %v", err)
	}

	return nil
}

func validateRequest(in *gitalypb.GetBlobRequest) error {
	if len(in.GetOid()) == 0 {
		return fmt.Errorf("empty Oid")
	}
	return nil
}
