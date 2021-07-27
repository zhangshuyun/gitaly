package commit

import (
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

type commitsSender struct {
	commits []*gitalypb.GitCommit
	send    func([]*gitalypb.GitCommit) error
}

func (s *commitsSender) Reset() {
	s.commits = s.commits[:0]
}

func (s *commitsSender) Append(m proto.Message) {
	s.commits = append(s.commits, m.(*gitalypb.GitCommit))
}

func (s *commitsSender) Send() error {
	return s.send(s.commits)
}
