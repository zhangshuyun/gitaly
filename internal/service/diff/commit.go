package diff

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/diff"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type requestWithLeftRightCommitIds interface {
	GetLeftCommitId() string
	GetRightCommitId() string
}

func (s *server) CommitDiff(in *gitalypb.CommitDiffRequest, stream gitalypb.DiffService_CommitDiffServer) error {
	ctxlogrus.Extract(stream.Context()).WithFields(log.Fields{
		"LeftCommitId":           in.LeftCommitId,
		"RightCommitId":          in.RightCommitId,
		"IgnoreWhitespaceChange": in.IgnoreWhitespaceChange,
		"Paths":                  logPaths(in.Paths),
	}).Debug("CommitDiff")

	if err := validateRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "CommitDiff: %v", err)
	}

	leftSha := in.LeftCommitId
	rightSha := in.RightCommitId
	ignoreWhitespaceChange := in.GetIgnoreWhitespaceChange()
	paths := in.GetPaths()

	cmd := git.SubCmd{
		Name: "diff",
		Flags: []git.Option{
			git.Flag{Name: "--patch"},
			git.Flag{Name: "--raw"},
			git.Flag{Name: "--abbrev=40"},
			git.Flag{Name: "--full-index"},
			git.Flag{Name: "--find-renames=30%"},
		},
		Args: []string{leftSha, rightSha},
	}

	if ignoreWhitespaceChange {
		cmd.Flags = append(cmd.Flags, git.Flag{Name: "--ignore-space-change"})
	}
	if len(paths) > 0 {
		for _, path := range paths {
			cmd.PostSepArgs = append(cmd.PostSepArgs, string(path))
		}
	}

	var limits diff.Limits
	if in.EnforceLimits {
		limits.EnforceLimits = true
		limits.MaxFiles = int(in.MaxFiles)
		limits.MaxLines = int(in.MaxLines)
		limits.MaxBytes = int(in.MaxBytes)
		limits.MaxPatchBytes = int(in.MaxPatchBytes)
	}
	limits.CollapseDiffs = in.CollapseDiffs
	limits.SafeMaxFiles = int(in.SafeMaxFiles)
	limits.SafeMaxLines = int(in.SafeMaxLines)
	limits.SafeMaxBytes = int(in.SafeMaxBytes)

	return eachDiff(stream.Context(), "CommitDiff", in.Repository, cmd, limits, func(diff *diff.Diff) error {
		response := &gitalypb.CommitDiffResponse{
			FromPath:       diff.FromPath,
			ToPath:         diff.ToPath,
			FromId:         diff.FromID,
			ToId:           diff.ToID,
			OldMode:        diff.OldMode,
			NewMode:        diff.NewMode,
			Binary:         diff.Binary,
			OverflowMarker: diff.OverflowMarker,
			Collapsed:      diff.Collapsed,
			TooLarge:       diff.TooLarge,
		}

		if len(diff.Patch) <= s.MsgSizeThreshold {
			response.RawPatchData = diff.Patch
			response.EndOfPatch = true

			if err := stream.Send(response); err != nil {
				return status.Errorf(codes.Unavailable, "CommitDiff: send: %v", err)
			}
		} else {
			patch := diff.Patch

			for len(patch) > 0 {
				if len(patch) > s.MsgSizeThreshold {
					response.RawPatchData = patch[:s.MsgSizeThreshold]
					patch = patch[s.MsgSizeThreshold:]
				} else {
					response.RawPatchData = patch
					response.EndOfPatch = true
					patch = nil
				}

				if err := stream.Send(response); err != nil {
					return status.Errorf(codes.Unavailable, "CommitDiff: send: %v", err)
				}

				// Use a new response so we don't send other fields (FromPath, ...) over and over
				response = &gitalypb.CommitDiffResponse{}
			}
		}

		return nil
	})
}

func (s *server) CommitDelta(in *gitalypb.CommitDeltaRequest, stream gitalypb.DiffService_CommitDeltaServer) error {
	ctxlogrus.Extract(stream.Context()).WithFields(log.Fields{
		"LeftCommitId":  in.LeftCommitId,
		"RightCommitId": in.RightCommitId,
		"Paths":         logPaths(in.Paths),
	}).Debug("CommitDelta")

	if err := validateRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "CommitDelta: %v", err)
	}

	leftSha := in.LeftCommitId
	rightSha := in.RightCommitId
	paths := in.GetPaths()

	cmd := git.SubCmd{
		Name: "diff",
		Flags: []git.Option{
			git.Flag{Name: "--raw"},
			git.Flag{Name: "--abbrev=40"},
			git.Flag{Name: "--full-index"},
			git.Flag{Name: "--find-renames"},
		},
		Args: []string{leftSha, rightSha},
	}
	if len(paths) > 0 {
		for _, path := range paths {
			cmd.PostSepArgs = append(cmd.PostSepArgs, string(path))
		}
	}

	var batch []*gitalypb.CommitDelta
	var batchSize int

	flushFunc := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := stream.Send(&gitalypb.CommitDeltaResponse{Deltas: batch}); err != nil {
			return status.Errorf(codes.Unavailable, "CommitDelta: send: %v", err)
		}

		return nil
	}

	err := eachDiff(stream.Context(), "CommitDelta", in.Repository, cmd, diff.Limits{}, func(diff *diff.Diff) error {
		delta := &gitalypb.CommitDelta{
			FromPath: diff.FromPath,
			ToPath:   diff.ToPath,
			FromId:   diff.FromID,
			ToId:     diff.ToID,
			OldMode:  diff.OldMode,
			NewMode:  diff.NewMode,
		}

		batch = append(batch, delta)
		batchSize += deltaSize(diff)

		if batchSize > s.MsgSizeThreshold {
			if err := flushFunc(); err != nil {
				return err
			}

			batch = nil
			batchSize = 0
		}

		return nil
	})

	if err != nil {
		return err
	}

	return flushFunc()
}

func validateRequest(in requestWithLeftRightCommitIds) error {
	if in.GetLeftCommitId() == "" {
		return fmt.Errorf("empty LeftCommitId")
	}
	if in.GetRightCommitId() == "" {
		return fmt.Errorf("empty RightCommitId")
	}

	return nil
}

func eachDiff(ctx context.Context, rpc string, repo *gitalypb.Repository, subCmd git.Cmd, limits diff.Limits, callback func(*diff.Diff) error) error {
	diffArgs := []git.Option{
		git.ValueFlag{"-c", "diff.noprefix=false"},
	}

	cmd, err := git.SafeCmd(ctx, repo, diffArgs, subCmd)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "%s: cmd: %v", rpc, err)
	}

	diffParser := diff.NewDiffParser(cmd, limits)

	for diffParser.Parse() {
		if err := callback(diffParser.Diff()); err != nil {
			return err
		}
	}

	if err := diffParser.Err(); err != nil {
		return status.Errorf(codes.Internal, "%s: parse failure: %v", rpc, err)
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Unavailable, "%s: %v", rpc, err)
	}

	return nil
}

func deltaSize(diff *diff.Diff) int {
	size := len(diff.FromID) + len(diff.ToID) +
		4 + 4 + // OldMode and NewMode are int32 = 32/8 = 4 bytes
		len(diff.FromPath) + len(diff.ToPath)

	return size
}

func logPaths(paths [][]byte) []string {
	result := make([]string, len(paths))
	for i, p := range paths {
		result[i] = string(p)
	}
	return result
}
