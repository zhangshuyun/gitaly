package diff

import (
	"log"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/diff"
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) CommitDiff(in *pb.CommitDiffRequest, stream pb.Diff_CommitDiffServer) error {
	if err := validateRequest(in); err != nil {
		return err
	}

	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}
	leftSha := in.LeftCommitId
	rightSha := in.RightCommitId

	log.Printf("CommitDiff: RepoPath=%q LeftCommitId=%q RightCommitId=%q", repoPath, leftSha, rightSha)

	cmd, err := helper.GitCommandReader(stream.Context(), "--git-dir", repoPath, "diff", "--full-index", "--find-renames", leftSha, rightSha)
	if err != nil {
		return grpc.Errorf(codes.Internal, "CommitDiff: cmd: %v", err)
	}
	defer cmd.Kill()

	diffParser := diff.NewDiffParser(cmd)

	for diffParser.Parse() {
		diff := diffParser.Diff()
		err = stream.Send(&pb.CommitDiffResponse{
			FromPath:  diff.FromPath,
			ToPath:    diff.ToPath,
			FromId:    diff.FromID,
			ToId:      diff.ToID,
			OldMode:   diff.OldMode,
			NewMode:   diff.NewMode,
			Binary:    diff.Binary,
			RawChunks: diff.RawChunks,
		})

		if err != nil {
			return grpc.Errorf(codes.Unavailable, "CommitDiff: send: %v", err)
		}
	}

	if err := diffParser.Err(); err != nil {
		log.Printf("CommitDiff: Parsing diff in repo %q between %q and %q failed: %v", repoPath, leftSha, rightSha, err)
		return grpc.Errorf(codes.Internal, "CommitDiff: parse failure: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return grpc.Errorf(codes.Unavailable, "CommitDiff: cmd wait for %v: %v", cmd.Args, err)
	}

	return nil
}

func validateRequest(in *pb.CommitDiffRequest) error {
	if in.LeftCommitId == "" {
		return grpc.Errorf(codes.InvalidArgument, "CommitDiff: empty LeftCommitId")
	}
	if in.RightCommitId == "" {
		return grpc.Errorf(codes.InvalidArgument, "CommitDiff: empty RightCommitId")
	}

	return nil
}
