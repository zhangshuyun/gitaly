package commit

import (
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	gitlog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

var (
	listCommitsbyOidHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "gitaly_list_commits_by_oid_request_size",
			Help: "Number of commits requested in a ListCommitsByOid request",

			// We want to count the pathological case where the request is empty. I
			// am not sure if with floats, Observe(0) would go into bucket 0. Use
			// bucket 0.001 because 0 <= 0.001 for sure.
			Buckets: []float64{0.001, 1, 5, 10, 20},
		})
)

func init() {
	prometheus.MustRegister(listCommitsbyOidHistogram)
}

func (s *server) ListCommitsByOid(in *gitalypb.ListCommitsByOidRequest, stream gitalypb.CommitService_ListCommitsByOidServer) error {
	ctx := stream.Context()

	c, err := catfile.New(ctx, s.locator, in.Repository)
	if err != nil {
		return err
	}

	sender := chunk.New(&commitsByOidSender{stream: stream})
	listCommitsbyOidHistogram.Observe(float64(len(in.Oid)))

	for _, oid := range in.Oid {
		commit, err := gitlog.GetCommitCatfile(ctx, c, git.Revision(oid))
		if catfile.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}

		if err := sender.Send(commit); err != nil {
			return err
		}
	}

	return sender.Flush()
}

type commitsByOidSender struct {
	response *gitalypb.ListCommitsByOidResponse
	stream   gitalypb.CommitService_ListCommitsByOidServer
}

func (c *commitsByOidSender) Append(m proto.Message) {
	c.response.Commits = append(c.response.Commits, m.(*gitalypb.GitCommit))
}

func (c *commitsByOidSender) Send() error { return c.stream.Send(c.response) }
func (c *commitsByOidSender) Reset()      { c.response = &gitalypb.ListCommitsByOidResponse{} }
