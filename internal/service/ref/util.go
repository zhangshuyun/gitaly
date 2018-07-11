package ref

import (
	"bytes"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var localBranchFormatFields = []string{"%(refname)", "%(objectname)"}

func parseRef(ref []byte) ([][]byte, error) {
	elements := bytes.Split(ref, []byte("\x00"))
	if len(elements) != len(localBranchFormatFields) {
		return nil, status.Errorf(codes.Internal, "error parsing ref %q", ref)
	}
	return elements, nil
}

func buildLocalBranch(name []byte, target *pb.GitCommit) *pb.FindLocalBranchResponse {
	response := &pb.FindLocalBranchResponse{
		Name:          name,
		CommitId:      target.Id,
		CommitSubject: target.Subject,
	}

	if author := target.Author; author != nil {
		response.CommitAuthor = &pb.FindLocalBranchCommitAuthor{
			Name:  author.Name,
			Email: author.Email,
			Date:  author.Date,
		}
	}

	if committer := target.Committer; committer != nil {
		response.CommitCommitter = &pb.FindLocalBranchCommitAuthor{
			Name:  committer.Name,
			Email: committer.Email,
			Date:  committer.Date,
		}
	}

	return response
}

func buildBranch(c *catfile.Batch, elements [][]byte) (*pb.FindAllBranchesResponse_Branch, error) {
	target, err := log.GetCommitCatfile(c, string(elements[1]))
	if err != nil {
		return nil, err
	}

	return &pb.FindAllBranchesResponse_Branch{
		Name:   elements[0],
		Target: target,
	}, nil
}

func newFindAllBranchNamesWriter(stream pb.Ref_FindAllBranchNamesServer) lines.Sender {
	return func(refs [][]byte) error {
		return stream.Send(&pb.FindAllBranchNamesResponse{Names: refs})
	}
}

func newFindAllTagNamesWriter(stream pb.Ref_FindAllTagNamesServer) lines.Sender {
	return func(refs [][]byte) error {
		return stream.Send(&pb.FindAllTagNamesResponse{Names: refs})
	}
}

func newFindLocalBranchesWriter(stream pb.Ref_FindLocalBranchesServer, c *catfile.Batch) lines.Sender {
	return func(refs [][]byte) error {
		var branches []*pb.FindLocalBranchResponse

		for _, ref := range refs {
			elements, err := parseRef(ref)
			if err != nil {
				return err
			}

			target, err := log.GetCommitCatfile(c, string(elements[1]))
			if err != nil {
				return err
			}

			branches = append(branches, buildLocalBranch(elements[0], target))
		}
		return stream.Send(&pb.FindLocalBranchesResponse{Branches: branches})
	}
}

func newFindAllBranchesWriter(stream pb.RefService_FindAllBranchesServer, c *catfile.Batch) lines.Sender {
	return func(refs [][]byte) error {
		var branches []*pb.FindAllBranchesResponse_Branch

		for _, ref := range refs {
			elements, err := parseRef(ref)
			if err != nil {
				return err
			}
			branch, err := buildBranch(c, elements)
			if err != nil {
				return err
			}
			branches = append(branches, branch)
		}
		return stream.Send(&pb.FindAllBranchesResponse{Branches: branches})
	}
}
