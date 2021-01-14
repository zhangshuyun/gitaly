package commit

import (
	"bytes"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (s *server) GetCommitMessages(request *gitalypb.GetCommitMessagesRequest, stream gitalypb.CommitService_GetCommitMessagesServer) error {
	if err := validateGetCommitMessagesRequest(request); err != nil {
		return helper.ErrInvalidArgument(err)
	}
	if err := s.getAndStreamCommitMessages(request, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) getAndStreamCommitMessages(request *gitalypb.GetCommitMessagesRequest, stream gitalypb.CommitService_GetCommitMessagesServer) error {
	ctx := stream.Context()
	c, err := catfile.New(ctx, s.locator, request.GetRepository())
	if err != nil {
		return err
	}
	for _, commitID := range request.GetCommitIds() {
		msg, err := log.GetCommitMessage(ctx, c, request.GetRepository(), git.Revision(commitID))
		if err != nil {
			return fmt.Errorf("failed to get commit message: %v", err)
		}
		msgReader := bytes.NewReader(msg)

		if err := stream.Send(&gitalypb.GetCommitMessagesResponse{CommitId: commitID}); err != nil {
			return err
		}
		sw := streamio.NewWriter(func(p []byte) error {
			return stream.Send(&gitalypb.GetCommitMessagesResponse{Message: p})
		})
		_, err = io.Copy(sw, msgReader)
		if err != nil {
			return fmt.Errorf("failed to send response: %v", err)
		}
	}
	return nil
}

func validateGetCommitMessagesRequest(request *gitalypb.GetCommitMessagesRequest) error {
	if request.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	return nil
}
