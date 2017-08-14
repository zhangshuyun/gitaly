package commit

import (
	"io/ioutil"
	"os/exec"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"

	log "github.com/Sirupsen/logrus"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

func (s *server) CommitIsAncestor(ctx context.Context, in *pb.CommitIsAncestorRequest) (*pb.CommitIsAncestorResponse, error) {
	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return nil, err
	}
	if in.AncestorId == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "Bad Request (empty ancestor sha)")
	}
	if in.ChildId == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "Bad Request (empty child sha)")
	}

	ret, err := commitIsAncestorName(ctx, repoPath, in.AncestorId, in.ChildId)
	return &pb.CommitIsAncestorResponse{Value: ret}, err
}

// Assumes that `path`, `ancestorID` and `childID` are populated :trollface:
func commitIsAncestorName(ctx context.Context, path, ancestorID, childID string) (bool, error) {
	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"ancestorSha": ancestorID,
		"childSha":    childID,
	}).Debug("commitIsAncestor")

	osCommand := exec.Command(helper.GitPath(), "--git-dir", path, "merge-base", "--is-ancestor", ancestorID, childID)
	cmd, err := helper.NewCommand(ctx, osCommand, nil, ioutil.Discard, nil)
	if err != nil {
		return false, grpc.Errorf(codes.Internal, err.Error())
	}
	defer cmd.CleanUpProcessGroup(ctx)

	return cmd.Wait() == nil, nil
}
