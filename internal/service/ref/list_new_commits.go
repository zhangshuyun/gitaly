package ref

import (
	"bufio"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
)

func (s *server) ListNewCommits(in *gitalypb.ListNewCommitsRequest, stream gitalypb.RefService_ListNewCommitsServer) error {
	oid := in.GetCommitId()
	if err := validateCommitID(oid); err != nil {
		return err
	}

	ctx := stream.Context()

	revList, err := git.Command(ctx, in.GetRepository(), "rev-list", oid, "--not", "--all")
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "ListNewCommits: gitCommand: %v", err)
	}

	batch, err := catfile.New(ctx, in.GetRepository())
	if err != nil {
		return status.Errorf(codes.Internal, "ListNewCommits: catfile: %v", err)
	}

	commits := []*gitalypb.GitCommit{}
	scanner := bufio.NewScanner(revList)
	for scanner.Scan() {
		line := scanner.Text()

		commit, err := log.GetCommitCatfile(batch, line)
		if err != nil {
			return status.Errorf(codes.Internal, "ListNewCommits: commit not found: %v", err)
		}
		commits = append(commits, commit)

		if len(commits) >= 10 {
			response := &gitalypb.ListNewCommitsResponse{Commits: commits}
			if err := stream.Send(response); err != nil {
				return err
			}

			commits = commits[:0]
		}
	}

	if len(commits) > 0 {
		response := &gitalypb.ListNewCommitsResponse{Commits: commits}
		if err := stream.Send(response); err != nil {
			return err
		}
	}

	return revList.Wait()
}
