package operations

import (
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"golang.org/x/net/context"
)

func (s *server) UserUpdateSubmodule(ctx context.Context, req *gitalypb.UserUpdateSubmoduleRequest) (*gitalypb.UserUpdateSubmoduleResponse, error) {
	return nil, helper.Unimplemented
}
