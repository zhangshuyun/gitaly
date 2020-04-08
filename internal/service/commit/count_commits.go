package commit

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CountCommits(ctx context.Context, in *gitalypb.CountCommitsRequest) (*gitalypb.CountCommitsResponse, error) {
	if err := validateCountCommitsRequest(in); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "CountCommits: %v", err)
	}

	subCmd := git.SubCmd{Name: "rev-list", Flags: []git.Option{git.Flag{Name: "--count"}}}

	if in.GetAll() {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--all"})
	} else {
		subCmd.Args = []string{string(in.GetRevision())}
	}

	if before := in.GetBefore(); before != nil {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--before=" + timestampToRFC3339(before.Seconds)})
	}
	if after := in.GetAfter(); after != nil {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--after=" + timestampToRFC3339(after.Seconds)})
	}
	if maxCount := in.GetMaxCount(); maxCount != 0 {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: fmt.Sprintf("--max-count=%d", maxCount)})
	}
	if in.GetFirstParent() {
		subCmd.Flags = append(subCmd.Flags, git.Flag{Name: "--first-parent"})
	}
	if path := in.GetPath(); path != nil {
		subCmd.PostSepArgs = []string{string(path)}
	}

	cmd, err := git.SafeCmd(ctx, in.Repository, nil, subCmd)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, "CountCommits: cmd: %v", err)
	}

	var count int64
	countStr, readAllErr := ioutil.ReadAll(cmd)
	if readAllErr != nil {
		ctxlogrus.Extract(ctx).WithError(err).Info("ignoring git rev-list error")
	}

	if err := cmd.Wait(); err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Info("ignoring git rev-list error")
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
