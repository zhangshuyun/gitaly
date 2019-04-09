package commit

import (
	"errors"
	"io"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	gitlog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

func (s *server) ListCommits(stream gitalypb.CommitService_ListCommitsServer) error {
	ctx := stream.Context()

	msg, err := stream.Recv()
	if err != nil {
		return helper.ErrInternal(err)
	}

	header := msg.GetHeader()
	if header == nil {
		return helper.ErrInvalidArgument(errors.New("expected the first message to be a header"))
	}

	catfile, err := catfile.New(ctx, header.Repository)
	if err != nil {
		return helper.ErrInternalf("error creating catfile: %v", err)
	}

	for msg, err = stream.Recv(); err != io.EOF; {
		if err != nil {
			return helper.ErrInternal(err)
		}

		rev := msg.GetRevision()
		if rev == nil {
			grpc_logrus.Extract(ctx).WithField("glRepository", header.Repository.GlRepository).Warn("revision was not send")
			continue
		}

		commit, err := gitlog.GetCommitCatfile(catfile, string(rev))
		if err != nil {
			return helper.ErrInternal(err)
		}

		resp := &gitalypb.ListCommitsResponse{Commit: commit}
		if err = stream.Send(resp); err != nil {
			return helper.ErrInternal(err)
		}
	}

	return nil
}
