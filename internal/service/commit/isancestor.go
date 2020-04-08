package commit

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CommitIsAncestor(ctx context.Context, in *gitalypb.CommitIsAncestorRequest) (*gitalypb.CommitIsAncestorResponse, error) {
	if in.AncestorId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty ancestor sha)")
	}
	if in.ChildId == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request (empty child sha)")
	}

	ret, err := commitIsAncestorName(ctx, in.Repository, in.AncestorId, in.ChildId)
	return &gitalypb.CommitIsAncestorResponse{Value: ret}, err
}

// Assumes that `path`, `ancestorID` and `childID` are populated :trollface:
func commitIsAncestorName(ctx context.Context, repo *gitalypb.Repository, ancestorID, childID string) (bool, error) {
	ctxlogrus.Extract(ctx).WithFields(log.Fields{
		"ancestorSha": ancestorID,
		"childSha":    childID,
	}).Debug("commitIsAncestor")

	cmd, err := git.SafeCmd(ctx, repo, nil, git.SubCmd{Name: "merge-base",
		Flags: []git.Option{git.Flag{Name: "--is-ancestor"}}, Args: []string{ancestorID, childID}})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return false, err
		}
		return false, status.Errorf(codes.Internal, err.Error())
	}

	return cmd.Wait() == nil, nil
}
