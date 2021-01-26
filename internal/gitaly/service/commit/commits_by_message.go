package commit

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type commitsByMessageSender struct {
	stream  gitalypb.CommitService_CommitsByMessageServer
	commits []*gitalypb.GitCommit
}

func (sender *commitsByMessageSender) Reset() { sender.commits = nil }
func (sender *commitsByMessageSender) Append(m proto.Message) {
	sender.commits = append(sender.commits, m.(*gitalypb.GitCommit))
}

func (sender *commitsByMessageSender) Send() error {
	return sender.stream.Send(&gitalypb.CommitsByMessageResponse{Commits: sender.commits})
}

func (s *server) CommitsByMessage(in *gitalypb.CommitsByMessageRequest, stream gitalypb.CommitService_CommitsByMessageServer) error {
	if err := validateCommitsByMessageRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := commitsByMessage(in, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func commitsByMessage(in *gitalypb.CommitsByMessageRequest, stream gitalypb.CommitService_CommitsByMessageServer) error {
	ctx := stream.Context()
	sender := &commitsByMessageSender{stream: stream}

	gitLogExtraOptions := []git.Option{
		git.Flag{Name: "--grep=" + in.GetQuery()},
		git.Flag{Name: "--regexp-ignore-case"},
	}
	if offset := in.GetOffset(); offset > 0 {
		gitLogExtraOptions = append(gitLogExtraOptions, git.Flag{Name: fmt.Sprintf("--skip=%d", offset)})
	}
	if limit := in.GetLimit(); limit > 0 {
		gitLogExtraOptions = append(gitLogExtraOptions, git.Flag{Name: fmt.Sprintf("--max-count=%d", limit)})
	}

	revision := in.GetRevision()
	if len(revision) == 0 {
		var err error

		revision, err = defaultBranchName(ctx, in.Repository)
		if err != nil {
			return err
		}
	}

	var paths []string
	if path := in.GetPath(); len(path) > 0 {
		paths = append(paths, string(path))
	}

	return sendCommits(stream.Context(), sender, in.GetRepository(), []string{string(revision)}, paths, in.GetGlobalOptions(), gitLogExtraOptions...)
}

func validateCommitsByMessageRequest(in *gitalypb.CommitsByMessageRequest) error {
	if err := git.ValidateRevisionAllowEmpty(in.Revision); err != nil {
		return err
	}

	if in.GetQuery() == "" {
		return fmt.Errorf("empty Query")
	}

	return nil
}
