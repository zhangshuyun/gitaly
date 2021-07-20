package commit

import (
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func verifyListCommitsRequest(request *gitalypb.ListCommitsRequest) error {
	if request.GetRepository() == nil {
		return errors.New("empty repository")
	}
	if len(request.GetRevisions()) == 0 {
		return errors.New("missing revisions")
	}
	for _, revision := range request.Revisions {
		if strings.HasPrefix(revision, "-") && revision != "--all" && revision != "--not" {
			return fmt.Errorf("invalid revision: %q", revision)
		}
	}
	return nil
}

func (s *server) ListCommits(
	request *gitalypb.ListCommitsRequest,
	stream gitalypb.CommitService_ListCommitsServer,
) error {
	if err := verifyListCommitsRequest(request); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	ctx := stream.Context()
	repo := s.localrepo(request.GetRepository())

	catfileProcess, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return helper.ErrInternal(fmt.Errorf("creating catfile process: %w", err))
	}

	revlistOptions := []gitpipe.RevlistOption{}

	switch request.GetOrder() {
	case gitalypb.ListCommitsRequest_NONE:
		// Nothing to do, we use default sorting.
	case gitalypb.ListCommitsRequest_TOPO:
		revlistOptions = append(revlistOptions, gitpipe.WithOrder(gitpipe.OrderTopo))
	case gitalypb.ListCommitsRequest_DATE:
		revlistOptions = append(revlistOptions, gitpipe.WithOrder(gitpipe.OrderDate))
	}

	if request.GetReverse() {
		revlistOptions = append(revlistOptions, gitpipe.WithReverse())
	}

	if request.GetMaxParents() > 0 {
		revlistOptions = append(revlistOptions, gitpipe.WithMaxParents(uint(request.GetMaxParents())))
	}

	if request.GetDisableWalk() {
		revlistOptions = append(revlistOptions, gitpipe.WithDisabledWalk())
	}

	if request.GetFirstParent() {
		revlistOptions = append(revlistOptions, gitpipe.WithFirstParent())
	}

	if request.GetBefore() != nil {
		revlistOptions = append(revlistOptions, gitpipe.WithBefore(request.GetBefore().AsTime()))
	}

	if request.GetAfter() != nil {
		revlistOptions = append(revlistOptions, gitpipe.WithAfter(request.GetAfter().AsTime()))
	}

	if len(request.GetAuthor()) != 0 {
		revlistOptions = append(revlistOptions, gitpipe.WithAuthor(request.GetAuthor()))
	}

	revlistIter := gitpipe.Revlist(ctx, repo, request.GetRevisions(), revlistOptions...)

	// If we've got a pagination token, then we will only start to print commits as soon as
	// we've seen the token.
	if token := request.GetPaginationParams().GetPageToken(); token != "" {
		tokenSeen := false
		revlistIter = gitpipe.RevisionFilter(ctx, revlistIter, func(r gitpipe.RevisionResult) bool {
			if !tokenSeen {
				tokenSeen = r.OID == git.ObjectID(token)
				// We also skip the token itself, thus we always return `false`
				// here.
				return false
			}

			return true
		})
	}

	catfileInfoIter := gitpipe.CatfileInfo(ctx, catfileProcess, revlistIter)
	catfileObjectIter := gitpipe.CatfileObject(ctx, catfileProcess, catfileInfoIter)

	chunker := chunk.New(&commitsSender{
		send: func(commits []*gitalypb.GitCommit) error {
			return stream.Send(&gitalypb.ListCommitsResponse{
				Commits: commits,
			})
		},
	})

	limit := request.GetPaginationParams().GetLimit()

	for i := int32(0); catfileObjectIter.Next(); i++ {
		// If we hit the pagination limit, then we stop sending commits even if there are
		// more commits in the pipeline.
		if limit > 0 && limit <= i {
			break
		}

		object := catfileObjectIter.Result()

		commit, err := catfile.ParseCommit(object.ObjectReader, object.ObjectInfo.Oid)
		if err != nil {
			return helper.ErrInternal(fmt.Errorf("parsing commit: %w", err))
		}

		if err := chunker.Send(commit); err != nil {
			return helper.ErrInternal(fmt.Errorf("sending commit: %w", err))
		}
	}

	if err := catfileObjectIter.Err(); err != nil {
		return helper.ErrInternal(fmt.Errorf("iterating objects: %w", err))
	}

	if err := chunker.Flush(); err != nil {
		return helper.ErrInternal(fmt.Errorf("flushing commits: %w", err))
	}

	return nil
}
