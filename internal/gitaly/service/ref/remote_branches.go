package ref

import (
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) FindAllRemoteBranches(req *gitalypb.FindAllRemoteBranchesRequest, stream gitalypb.RefService_FindAllRemoteBranchesServer) error {
	if err := validateFindAllRemoteBranchesRequest(req); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := findAllRemoteBranches(req, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func findAllRemoteBranches(req *gitalypb.FindAllRemoteBranchesRequest, stream gitalypb.RefService_FindAllRemoteBranchesServer) error {
	args := []git.Option{
		git.Flag{Name: "--format=" + strings.Join(localBranchFormatFields, "%00")},
	}

	patterns := []string{"refs/remotes/" + req.GetRemoteName()}

	ctx := stream.Context()
	c, err := catfile.New(ctx, req.GetRepository())
	if err != nil {
		return err
	}

	opts := paginationParamsToOpts(nil)
	opts.cmdArgs = args
	writer := newFindAllRemoteBranchesWriter(stream, c)

	return findRefs(ctx, writer, req.GetRepository(), patterns, opts)
}

func validateFindAllRemoteBranchesRequest(req *gitalypb.FindAllRemoteBranchesRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if len(req.GetRemoteName()) == 0 {
		return fmt.Errorf("empty RemoteName")
	}

	return nil
}
