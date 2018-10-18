package commit

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
)

type commitsBetweenSender struct {
	stream gitalypb.CommitService_CommitsBetweenServer
}

func (s *server) CommitsBetween(in *gitalypb.CommitsBetweenRequest, stream gitalypb.CommitService_CommitsBetweenServer) error {
	if err := git.ValidateRevision(in.GetFrom()); err != nil {
		return status.Errorf(codes.InvalidArgument, "CommitsBetween: from: %v", err)
	}
	if err := git.ValidateRevision(in.GetTo()); err != nil {
		return status.Errorf(codes.InvalidArgument, "CommitsBetween: to: %v", err)
	}

	sender := &commitsBetweenSender{stream}
	revisionRange := fmt.Sprintf("%s..%s", in.GetFrom(), in.GetTo())

	return sendCommits(stream.Context(), sender, in.GetRepository(), []string{revisionRange}, nil, "--reverse")
}

func (sender *commitsBetweenSender) Send(commits []*gitalypb.GitCommit) error {
	return sender.stream.Send(&gitalypb.CommitsBetweenResponse{Commits: commits})
}
