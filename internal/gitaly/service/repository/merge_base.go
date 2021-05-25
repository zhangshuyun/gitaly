package repository

import (
	"context"
	"io/ioutil"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) FindMergeBase(ctx context.Context, req *gitalypb.FindMergeBaseRequest) (*gitalypb.FindMergeBaseResponse, error) {
	var revisions []string
	for _, rev := range req.GetRevisions() {
		revisions = append(revisions, string(rev))
	}

	if len(revisions) < 2 {
		return nil, status.Errorf(codes.InvalidArgument, "FindMergeBase: at least 2 revisions are required")
	}

	cmd, err := s.gitCmdFactory.New(ctx, req.GetRepository(),
		git.SubCmd{
			Name: "merge-base",
			Args: revisions,
		},
	)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "FindMergeBase: cmd: %v", err)
	}

	mergeBase, err := ioutil.ReadAll(cmd)
	if err != nil {
		return nil, err
	}

	mergeBaseStr := text.ChompBytes(mergeBase)

	if err := cmd.Wait(); err != nil {
		// On error just return an empty merge base
		return &gitalypb.FindMergeBaseResponse{Base: ""}, nil
	}

	return &gitalypb.FindMergeBaseResponse{Base: mergeBaseStr}, nil
}
