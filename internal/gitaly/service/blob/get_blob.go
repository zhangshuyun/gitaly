package blob

import (
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
)

func (s *server) GetBlob(in *gitalypb.GetBlobRequest, stream gitalypb.BlobService_GetBlobServer) error {
	ctx := stream.Context()

	repo := s.localrepo(in.GetRepository())

	if err := validateRequest(in); err != nil {
		return helper.ErrInvalidArgumentf("GetBlob: %v", err)
	}

	c, err := s.catfileCache.BatchProcess(stream.Context(), repo)
	if err != nil {
		return helper.ErrInternalf("GetBlob: %v", err)
	}

	objectInfo, err := c.Info(ctx, git.Revision(in.Oid))
	if err != nil && !catfile.IsNotFound(err) {
		return helper.ErrInternalf("GetBlob: %v", err)
	}
	if catfile.IsNotFound(err) || objectInfo.Type != "blob" {
		return helper.ErrUnavailable(stream.Send(&gitalypb.GetBlobResponse{}))
	}

	readLimit := objectInfo.Size
	if in.Limit >= 0 && in.Limit < readLimit {
		readLimit = in.Limit
	}
	firstMessage := &gitalypb.GetBlobResponse{
		Size: objectInfo.Size,
		Oid:  objectInfo.Oid.String(),
	}

	if readLimit == 0 {
		return helper.ErrUnavailable(stream.Send(firstMessage))
	}

	blobObj, err := c.Blob(ctx, git.Revision(objectInfo.Oid))
	if err != nil {
		return helper.ErrInternalf("GetBlob: %v", err)
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
		return helper.ErrUnavailablef("GetBlob: send: %v", err)
	}

	return nil
}

func validateRequest(in *gitalypb.GetBlobRequest) error {
	if len(in.GetOid()) == 0 {
		return fmt.Errorf("empty Oid")
	}
	return nil
}
