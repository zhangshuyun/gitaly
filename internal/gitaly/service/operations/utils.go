package operations

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

type userTimestampProto interface {
	GetUser() *gitalypb.User
	GetTimestamp() *timestamppb.Timestamp
}

func dateFromProto(p userTimestampProto) (time.Time, error) {
	date := time.Now()

	if timestamp := p.GetTimestamp(); timestamp != nil {
		var err error
		date, err = ptypes.Timestamp(timestamp)
		if err != nil {
			return time.Time{}, err
		}
	}

	if user := p.GetUser(); user != nil {
		location, err := time.LoadLocation(user.GetTimezone())
		if err != nil {
			return time.Time{}, err
		}
		date = date.In(location)
	}

	return date, nil
}
