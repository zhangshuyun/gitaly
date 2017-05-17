package commit

import (
	"io/ioutil"
	"os/exec"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

func (s *server) CommitIsAncestor(ctx context.Context, in *pb.CommitIsAncestorRequest) (*pb.CommitIsAncestorResponse, error) {
	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return nil, err
	}
	if in.AncestorId == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "bad request (empty ancestor sha)")
	}
	if in.ChildId == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "bad request (empty child sha)")
	}

	ret, err := commitIsAncestorName(repoPath, in.AncestorId, in.ChildId)
	return &pb.CommitIsAncestorResponse{Value: ret}, err
}

// Assumes that `path`, `ancestorID` and `childID` are populated :trollface:
func commitIsAncestorName(path, ancestorID, childID string) (bool, error) {
	osCommand := exec.Command("git", "--git-dir", path, "merge-base", "--is-ancestor", ancestorID, childID)
	cmd, err := helper.NewCommand(osCommand, nil, ioutil.Discard, nil)
	if err != nil {
		return false, grpc.Errorf(codes.Internal, err.Error())
	}
	defer cmd.Kill()

	helper.Debugf("commitIsAncestor: RepoPath=%q ancestorSha=%s childSha=%s", path, ancestorID, childID)
	return cmd.Wait() == nil, nil
}
