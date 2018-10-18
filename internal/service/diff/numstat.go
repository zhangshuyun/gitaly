package diff

import (
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/diff"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

var (
	maxNumStatBatchSize = 1000
)

func (s *server) DiffStats(in *gitalypb.DiffStatsRequest, stream gitalypb.DiffService_DiffStatsServer) error {
	if err := validateDiffStatsRequestParams(in); err != nil {
		return err
	}

	var batch []*gitalypb.DiffStats
	cmdArgs := []string{"diff", "--numstat", "-z", in.LeftCommitId, in.RightCommitId}
	cmd, err := git.Command(stream.Context(), in.Repository, cmdArgs...)

	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "%s: cmd: %v", "DiffStats", err)
	}

	parser := diff.NewDiffNumStatParser(cmd)

	for {
		stat, err := parser.NextNumStat()
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		numStat := &gitalypb.DiffStats{
			Additions: stat.Additions,
			Deletions: stat.Deletions,
			Path:      stat.Path,
		}

		batch = append(batch, numStat)

		if len(batch) == maxNumStatBatchSize {
			if err := sendStats(batch, stream); err != nil {
				return err
			}

			batch = nil
		}
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Unavailable, "%s: %v", "DiffStats", err)
	}

	return sendStats(batch, stream)
}

func sendStats(batch []*gitalypb.DiffStats, stream gitalypb.DiffService_DiffStatsServer) error {
	if len(batch) == 0 {
		return nil
	}

	if err := stream.Send(&gitalypb.DiffStatsResponse{Stats: batch}); err != nil {
		return status.Errorf(codes.Unavailable, "DiffStats: send: %v", err)
	}

	return nil
}

func validateDiffStatsRequestParams(in *gitalypb.DiffStatsRequest) error {
	repo := in.GetRepository()
	if _, err := helper.GetRepoPath(repo); err != nil {
		return err
	}

	if err := validateRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "DiffStats: %v", err)
	}

	return nil
}
