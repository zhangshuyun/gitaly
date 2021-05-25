package commit

import (
	"fmt"
	"strconv"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type commitsBetweenSender struct {
	stream  gitalypb.CommitService_CommitsBetweenServer
	commits []*gitalypb.GitCommit
}

func (sender *commitsBetweenSender) Reset() { sender.commits = nil }
func (sender *commitsBetweenSender) Append(m proto.Message) {
	sender.commits = append(sender.commits, m.(*gitalypb.GitCommit))
}

func (sender *commitsBetweenSender) Send() error {
	return sender.stream.Send(&gitalypb.CommitsBetweenResponse{Commits: sender.commits})
}

func (s *server) CommitsBetween(in *gitalypb.CommitsBetweenRequest, stream gitalypb.CommitService_CommitsBetweenServer) error {
	if err := validateCommitsBetween(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	repo := s.localrepo(in.GetRepository())
	sender := &commitsBetweenSender{stream: stream}

	from, to, limit := normalizedCommitsBetweenParams(in)
	revisionRange := fmt.Sprintf("%s..%s", from, to)

	if err := s.sendCommits(
		stream.Context(),
		sender,
		repo,
		[]string{revisionRange},
		nil,
		nil,
		git.Flag{Name: "--reverse"},
		git.ValueFlag{Name: "--max-count", Value: strconv.Itoa(int(limit))},
	); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

// returns the from, to, and limit CLI parameters abiding by pagination params
func normalizedCommitsBetweenParams(req *gitalypb.CommitsBetweenRequest) (string, string, int32) {
	from := string(req.GetFrom())
	to := string(req.GetTo())
	var limit int32 = 2147483647

	if req.PaginationParams != nil {
		from = req.PaginationParams.GetPageToken() + "~"

		if req.PaginationParams.GetLimit() > 0 {
			limit = req.PaginationParams.GetLimit()
		}
	}

	return from, to, limit
}

func validateCommitsBetween(in *gitalypb.CommitsBetweenRequest) error {
	if err := git.ValidateRevision(in.GetFrom()); err != nil {
		return fmt.Errorf("from: %v", err)
	}

	if err := git.ValidateRevision(in.GetTo()); err != nil {
		return fmt.Errorf("to: %v", err)
	}

	return nil
}
