package commit

import (
	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) ListCommitsByRefName(in *gitalypb.ListCommitsByRefNameRequest, stream gitalypb.CommitService_ListCommitsByRefNameServer) error {
	ctx := stream.Context()
	repo := s.localrepo(in.GetRepository())

	c, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return helper.ErrInternal(err)
	}

	sender := chunk.New(&commitsByRefNameSender{stream: stream})

	for _, refName := range in.RefNames {
		commit, err := catfile.GetCommit(ctx, c, git.Revision(refName))
		if catfile.IsNotFound(err) {
			continue
		}
		if err != nil {
			return helper.ErrInternal(err)
		}

		commitByRef := &gitalypb.ListCommitsByRefNameResponse_CommitForRef{
			Commit: commit, RefName: refName,
		}

		if err := sender.Send(commitByRef); err != nil {
			return helper.ErrInternal(err)
		}
	}

	return sender.Flush()
}

type commitsByRefNameSender struct {
	response *gitalypb.ListCommitsByRefNameResponse
	stream   gitalypb.CommitService_ListCommitsByRefNameServer
}

func (c *commitsByRefNameSender) Append(m proto.Message) {
	commitByRef := m.(*gitalypb.ListCommitsByRefNameResponse_CommitForRef)

	c.response.CommitRefs = append(c.response.CommitRefs, commitByRef)
}

func (c *commitsByRefNameSender) Send() error { return c.stream.Send(c.response) }
func (c *commitsByRefNameSender) Reset()      { c.response = &gitalypb.ListCommitsByRefNameResponse{} }
