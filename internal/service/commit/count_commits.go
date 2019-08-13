package commit

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CountCommits(ctx context.Context, in *gitalypb.CountCommitsRequest) (*gitalypb.CountCommitsResponse, error) {
	if err := validateCountCommitsRequest(in); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "CountCommits: %v", err)
	}

	cmdArgs := []string{"rev-list", "--count"}

	if in.GetAll() {
		cmdArgs = append(cmdArgs, "--all")
	} else {
		cmdArgs = append(cmdArgs, string(in.GetRevision()))
	}

	if before := in.GetBefore(); before != nil {
		cmdArgs = append(cmdArgs, "--before="+timestampToRFC3339(before.Seconds))
	}
	if after := in.GetAfter(); after != nil {
		cmdArgs = append(cmdArgs, "--after="+timestampToRFC3339(after.Seconds))
	}
	if maxCount := in.GetMaxCount(); maxCount != 0 {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--max-count=%d", maxCount))
	}
	if path := in.GetPath(); path != nil {
		cmdArgs = append(cmdArgs, "--", string(path))
	}

	cmd, err := git.Command(ctx, in.Repository, cmdArgs...)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "CountCommits: cmd: %v", err)
	}

	var count int64
	countStr, readAllErr := ioutil.ReadAll(cmd)
	if readAllErr != nil {
		grpc_logrus.Extract(ctx).WithError(err).Info("ignoring git rev-list error")
	}

	if err := cmd.Wait(); err != nil {
		grpc_logrus.Extract(ctx).WithError(err).Info("ignoring git rev-list error")
		count = 0
	} else if readAllErr == nil {
		var err error
		countStr = bytes.TrimSpace(countStr)
		count, err = strconv.ParseInt(string(countStr), 10, 0)

		if err != nil {
			return nil, status.Errorf(codes.Internal, "CountCommits: parse count: %v", err)
		}
	}

	return &gitalypb.CountCommitsResponse{Count: int32(count)}, nil
}

func validateCountCommitsRequest(in *gitalypb.CountCommitsRequest) error {
	if err := git.ValidateRevisionAllowEmpty(in.Revision); err != nil {
		return err
	}

	if len(in.GetRevision()) == 0 && !in.GetAll() {
		return fmt.Errorf("empty Revision and false All")
	}

	return nil
}

func timestampToRFC3339(ts int64) string {
	return time.Unix(ts, 0).Format(time.RFC3339)
}
