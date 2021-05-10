package ref

import (
	"bufio"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) ListNewCommits(in *gitalypb.ListNewCommitsRequest, stream gitalypb.RefService_ListNewCommitsServer) error {
	oid := in.GetCommitId()
	if err := git.ValidateObjectID(oid); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := s.listNewCommits(in, stream, oid); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) listNewCommits(in *gitalypb.ListNewCommitsRequest, stream gitalypb.RefService_ListNewCommitsServer, oid string) error {
	ctx := stream.Context()

	repo := s.localrepo(in.GetRepository())

	revList, err := repo.Exec(ctx, git.SubCmd{
		Name:  "rev-list",
		Flags: []git.Option{git.Flag{Name: "--not"}, git.Flag{Name: "--all"}},
		Args:  []string{"^" + oid}, // the added ^ is to negate the oid since there is a --not option that comes earlier in the arg list
	})
	if err != nil {
		return err
	}

	batch, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return err
	}

	commits := []*gitalypb.GitCommit{}
	scanner := bufio.NewScanner(revList)
	for scanner.Scan() {
		line := scanner.Text()

		commit, err := catfile.GetCommit(ctx, batch, git.Revision(line))
		if err != nil {
			return err
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
