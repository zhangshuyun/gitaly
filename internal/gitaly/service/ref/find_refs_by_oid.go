package ref

import (
	"context"
	"errors"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) FindRefsByOID(ctx context.Context, in *gitalypb.FindRefsByOIDRequest) (*gitalypb.FindRefsByOIDResponse, error) {
	if err := validateFindRefsReq(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	repo := s.localrepo(in.GetRepository())

	patterns := in.GetRefPatterns()
	if len(patterns) == 0 {
		patterns = []string{"refs/tags/", "refs/heads/"}
	}

	forEachRefIter := gitpipe.ForEachRef(
		ctx,
		repo,
		patterns,
		gitpipe.WithSortField(in.GetSortField()),
		gitpipe.WithPointsAt(in.GetOid()),
		gitpipe.WithCount(int(in.GetLimit())),
	)

	var refs []string
	for forEachRefIter.Next() {
		refs = append(refs, string(forEachRefIter.Result().ObjectName))
	}

	if err := forEachRefIter.Err(); err != nil {
		// git uses exit status 129 to indicate errors in command line usage
		// https://www.git-scm.com/docs/api-error-handling
		if strings.Contains(err.Error(), "exit status 129") {
			return nil, helper.ErrInvalidArgument(err)
		}
		return nil, err
	}

	return &gitalypb.FindRefsByOIDResponse{
		Refs: refs,
	}, nil
}

func validateFindRefsReq(in *gitalypb.FindRefsByOIDRequest) error {
	if in.GetRepository() == nil {
		return errors.New("empty Repository")
	}

	if in.GetOid() == "" {
		return errors.New("empty Oid")
	}

	return nil
}
