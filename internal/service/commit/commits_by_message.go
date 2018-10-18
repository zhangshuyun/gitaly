package commit

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
)

type commitsByMessageSender struct {
	stream gitalypb.CommitService_CommitsByMessageServer
}

func (s *server) CommitsByMessage(in *gitalypb.CommitsByMessageRequest, stream gitalypb.CommitService_CommitsByMessageServer) error {
	if err := validateCommitsByMessageRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "CommitsByMessage: %v", err)
	}

	ctx := stream.Context()
	sender := &commitsByMessageSender{stream}

	gitLogExtraOptions := []string{
		"--grep=" + in.GetQuery(),
		"--regexp-ignore-case",
	}
	if offset := in.GetOffset(); offset > 0 {
		gitLogExtraOptions = append(gitLogExtraOptions, fmt.Sprintf("--skip=%d", offset))
	}
	if limit := in.GetLimit(); limit > 0 {
		gitLogExtraOptions = append(gitLogExtraOptions, fmt.Sprintf("--max-count=%d", limit))
	}

	revision := in.GetRevision()
	if len(revision) == 0 {
		var err error

		revision, err = defaultBranchName(ctx, in.Repository)
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return err
			}
			return status.Errorf(codes.Internal, "CommitsByMessage: defaultBranchName: %v", err)
		}
	}

	var paths []string
	if path := in.GetPath(); len(path) > 0 {
		paths = append(paths, string(path))
	}

	return sendCommits(stream.Context(), sender, in.GetRepository(), []string{string(revision)}, paths, gitLogExtraOptions...)
}

func validateCommitsByMessageRequest(in *gitalypb.CommitsByMessageRequest) error {
	if in.GetQuery() == "" {
		return fmt.Errorf("empty Query")
	}

	return nil
}

func (sender *commitsByMessageSender) Send(commits []*gitalypb.GitCommit) error {
	return sender.stream.Send(&gitalypb.CommitsByMessageResponse{Commits: commits})
}
