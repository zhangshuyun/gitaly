package wiki

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type requestWithCommitDetails interface {
	GetCommitDetails() *gitalypb.WikiCommitDetails
}

func validateRequestCommitDetails(request requestWithCommitDetails) error {
	commitDetails := request.GetCommitDetails()
	if commitDetails == nil {
		return fmt.Errorf("empty CommitDetails")
	}

	if len(commitDetails.GetName()) == 0 {
		return fmt.Errorf("empty CommitDetails.Name")
	}

	if len(commitDetails.GetEmail()) == 0 {
		return fmt.Errorf("empty CommitDetails.Email")
	}

	if len(commitDetails.GetMessage()) == 0 {
		return fmt.Errorf("empty CommitDetails.Message")
	}

	return nil
}
