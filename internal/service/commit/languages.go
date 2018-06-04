package commit

import (
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/service/ref"

	"golang.org/x/net/context"
)

func (s *server) CommitLanguages(ctx context.Context, req *pb.CommitLanguagesRequest) (*pb.CommitLanguagesResponse, error) {
	repo := req.Repository

	if len(req.Revision) == 0 {
		defaultBranch, err := ref.DefaultBranchName(ctx, req.Repository)
		if err != nil {
			return nil, err
		}

		req.Revision = defaultBranch
	}

	client, err := s.CommitServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, repo)
	if err != nil {
		return nil, err
	}

	return client.CommitLanguages(clientCtx, req)
}
