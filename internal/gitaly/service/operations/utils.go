package operations

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type cherryPickOrRevertRequest interface {
	GetUser() *gitalypb.User
	GetCommit() *gitalypb.GitCommit
	GetBranchName() []byte
	GetMessage() []byte
}

func validateCherryPickOrRevertRequest(req cherryPickOrRevertRequest) error {
	if req.GetUser() == nil {
		return fmt.Errorf("empty User")
	}

	if req.GetCommit() == nil {
		return fmt.Errorf("empty Commit")
	}

	if len(req.GetBranchName()) == 0 {
		return fmt.Errorf("empty BranchName")
	}

	if len(req.GetMessage()) == 0 {
		return fmt.Errorf("empty Message")
	}

	return nil
}
