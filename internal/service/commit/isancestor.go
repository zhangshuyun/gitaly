package commit

import (
	"io/ioutil"
	"log"

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
		message := "Bad Request (empty ancestor sha)"
		log.Printf("CommitIsAncestor: %q", message)
		return nil, grpc.Errorf(codes.InvalidArgument, message)
	}
	if in.ChildId == "" {
		message := "Bad Request (empty child sha)"
		log.Printf("CommitIsAncestor: %q", message)
		return nil, grpc.Errorf(codes.InvalidArgument, message)
	}

	ret, err := commitIsAncestorName(ctx, repoPath, in.AncestorId, in.ChildId)
	return &pb.CommitIsAncestorResponse{Value: ret}, err
}

// Assumes that `path`, `ancestorID` and `childID` are populated :trollface:
func commitIsAncestorName(ctx context.Context, path, ancestorID, childID string) (bool, error) {
	osCommand := helper.CommandWrapper(ctx, "git", "--git-dir", path, "merge-base", "--is-ancestor", ancestorID, childID)
	cmd, err := helper.NewCommand(osCommand, nil, ioutil.Discard)
	if err != nil {
		return false, grpc.Errorf(codes.Internal, err.Error())
	}
	defer cmd.Kill()

	log.Printf("commitIsAncestor: RepoPath=%q ancestorSha=%s childSha=%s", path, ancestorID, childID)

	return cmd.Wait() == nil, nil
}
