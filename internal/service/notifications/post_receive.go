package notifications

import (
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"golang.org/x/net/context"
)

func (s *server) PostReceive(ctx context.Context, in *pb.PostReceiveRequest) (*pb.PostReceiveResponse, error) {
	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return nil, err
	}
	helper.Debugf("PostReceive: RepoPath=%q", repoPath)
	return &pb.PostReceiveResponse{}, nil
}
