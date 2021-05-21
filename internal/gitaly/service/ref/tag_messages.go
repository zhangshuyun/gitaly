package ref

import (
	"bytes"
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

func (s *server) GetTagMessages(request *gitalypb.GetTagMessagesRequest, stream gitalypb.RefService_GetTagMessagesServer) error {
	if err := validateGetTagMessagesRequest(request); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetTagMessages: %v", err)
	}
	if err := s.getAndStreamTagMessages(request, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func validateGetTagMessagesRequest(request *gitalypb.GetTagMessagesRequest) error {
	if request.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	return nil
}

func (s *server) getAndStreamTagMessages(request *gitalypb.GetTagMessagesRequest, stream gitalypb.RefService_GetTagMessagesServer) error {
	ctx := stream.Context()
	repo := s.localrepo(request.GetRepository())

	c, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return err
	}

	for _, tagID := range request.GetTagIds() {
		tag, err := catfile.GetTag(ctx, c, git.Revision(tagID), "", false, false)
		if err != nil {
			return fmt.Errorf("failed to get tag: %v", err)
		}

		if err := stream.Send(&gitalypb.GetTagMessagesResponse{TagId: tagID}); err != nil {
			return err
		}
		sw := streamio.NewWriter(func(p []byte) error {
			return stream.Send(&gitalypb.GetTagMessagesResponse{Message: p})
		})

		msgReader := bytes.NewReader(tag.Message)

		if _, err = io.Copy(sw, msgReader); err != nil {
			return fmt.Errorf("failed to send response: %v", err)
		}
	}
	return nil
}
