package commit

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// We declare this function in variables so that we can override them in our tests
var _findBranchNamesFunc = ref.FindBranchNames

func (s *server) FindAllCommits(in *gitalypb.FindAllCommitsRequest, stream gitalypb.CommitService_FindAllCommitsServer) error {
	if err := validateFindAllCommitsRequest(in); err != nil {
		return err
	}

	repo := s.localrepo(in.GetRepository())

	var revisions []string
	if len(in.GetRevision()) == 0 {
		branchNames, err := _findBranchNamesFunc(stream.Context(), repo)
		if err != nil {
			return helper.ErrInvalidArgument(err)
		}

		for _, branch := range branchNames {
			revisions = append(revisions, string(branch))
		}
	} else {
		revisions = []string{string(in.GetRevision())}
	}

	if err := s.findAllCommits(repo, in, stream, revisions); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func validateFindAllCommitsRequest(in *gitalypb.FindAllCommitsRequest) error {
	if err := git.ValidateRevisionAllowEmpty(in.Revision); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	return nil
}

func (s *server) findAllCommits(repo git.RepositoryExecutor, in *gitalypb.FindAllCommitsRequest, stream gitalypb.CommitService_FindAllCommitsServer, revisions []string) error {
	sender := &commitsSender{
		send: func(commits []*gitalypb.GitCommit) error {
			return stream.Send(&gitalypb.FindAllCommitsResponse{
				Commits: commits,
			})
		},
	}

	var gitLogExtraOptions []git.Option
	if maxCount := in.GetMaxCount(); maxCount > 0 {
		gitLogExtraOptions = append(gitLogExtraOptions, git.Flag{Name: fmt.Sprintf("--max-count=%d", maxCount)})
	}
	if skip := in.GetSkip(); skip > 0 {
		gitLogExtraOptions = append(gitLogExtraOptions, git.Flag{Name: fmt.Sprintf("--skip=%d", skip)})
	}
	switch in.GetOrder() {
	case gitalypb.FindAllCommitsRequest_NONE:
		// Do nothing
	case gitalypb.FindAllCommitsRequest_DATE:
		gitLogExtraOptions = append(gitLogExtraOptions, git.Flag{Name: "--date-order"})
	case gitalypb.FindAllCommitsRequest_TOPO:
		gitLogExtraOptions = append(gitLogExtraOptions, git.Flag{Name: "--topo-order"})
	}

	return s.sendCommits(stream.Context(), sender, repo, revisions, nil, nil, gitLogExtraOptions...)
}
