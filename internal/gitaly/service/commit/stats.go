package commit

import (
	"bufio"
	"context"
	"fmt"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) CommitStats(ctx context.Context, in *gitalypb.CommitStatsRequest) (*gitalypb.CommitStatsResponse, error) {
	if err := git.ValidateRevision(in.Revision); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	resp, err := commitStats(ctx, in)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	return resp, nil
}

func commitStats(ctx context.Context, in *gitalypb.CommitStatsRequest) (*gitalypb.CommitStatsResponse, error) {
	commit, err := log.GetCommit(ctx, in.Repository, string(in.Revision))
	if err != nil {
		return nil, err
	}
	if commit == nil {
		return nil, fmt.Errorf("commit not found: %q", in.Revision)
	}

	var args []string

	if len(commit.GetParentIds()) == 0 {
		args = append(args, git.EmptyTreeID, commit.Id)
	} else {
		args = append(args, commit.Id+"^", commit.Id)
	}

	cmd, err := git.SafeCmd(ctx, in.Repository, nil, git.SubCmd{
		Name:  "diff",
		Flags: []git.Option{git.Flag{Name: "--numstat"}},
		Args:  args,
	})
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(cmd)
	var added, deleted int32

	for scanner.Scan() {
		split := strings.SplitN(scanner.Text(), "\t", 3)
		if len(split) != 3 {
			return nil, fmt.Errorf("invalid numstat line %q", scanner.Text())
		}

		if split[0] == "-" && split[1] == "-" {
			// binary file
			continue
		}

		add64, err := strconv.ParseInt(split[0], 10, 32)
		if err != nil {
			return nil, err
		}

		added += int32(add64)

		del64, err := strconv.ParseInt(split[1], 10, 32)
		if err != nil {
			return nil, err
		}

		deleted += int32(del64)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return &gitalypb.CommitStatsResponse{
		Oid:       commit.Id,
		Additions: added,
		Deletions: deleted,
	}, nil
}
