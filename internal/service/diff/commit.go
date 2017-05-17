package diff

import (
	"fmt"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/diff"
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type requestWithLeftRightCommitIds interface {
	GetLeftCommitId() string
	GetRightCommitId() string
}

func (s *server) CommitDiff(in *pb.CommitDiffRequest, stream pb.Diff_CommitDiffServer) error {
	if err := validateRequest(in); err != nil {
		return grpc.Errorf(codes.InvalidArgument, "CommitDiff: %v", err)
	}

	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}
	leftSha := in.LeftCommitId
	rightSha := in.RightCommitId
	ignoreWhitespaceChange := in.GetIgnoreWhitespaceChange()
	paths := in.GetPaths()

	helper.Debugf(
		"CommitDiff: RepoPath=%q LeftCommitId=%q RightCommitId=%q IgnoreWhitespaceChange=%t Paths=%s",
		repoPath,
		leftSha,
		rightSha,
		ignoreWhitespaceChange,
		paths,
	)

	cmdArgs := []string{
		"--git-dir", repoPath,
		"diff",
		"--patch",
		"--raw",
		"--abbrev=40",
		"--full-index",
		"--find-renames",
	}
	if ignoreWhitespaceChange {
		cmdArgs = append(cmdArgs, "--ignore-space-change")
	}
	cmdArgs = append(cmdArgs, leftSha, rightSha)
	if len(paths) > 0 {
		cmdArgs = append(cmdArgs, "--")
		for _, path := range paths {
			cmdArgs = append(cmdArgs, string(path))
		}
	}

	err = eachDiff("CommitDiff", cmdArgs, func(diff *diff.Diff) error {
		err := stream.Send(&pb.CommitDiffResponse{
			FromPath:  diff.FromPath,
			ToPath:    diff.ToPath,
			FromId:    diff.FromID,
			ToId:      diff.ToID,
			OldMode:   diff.OldMode,
			NewMode:   diff.NewMode,
			Binary:    diff.Binary,
			RawChunks: diff.RawChunks,
		})

		if err != nil {
			return grpc.Errorf(codes.Unavailable, "CommitDiff: send: %v", err)
		}

		return nil
	})

	return err
}

func (s *server) CommitDelta(in *pb.CommitDeltaRequest, stream pb.Diff_CommitDeltaServer) error {
	if err := validateRequest(in); err != nil {
		return grpc.Errorf(codes.InvalidArgument, "CommitDelta: %v", err)
	}

	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}
	leftSha := in.LeftCommitId
	rightSha := in.RightCommitId
	paths := in.GetPaths()

	helper.Debugf(
		"CommitDelta: RepoPath=%q LeftCommitId=%q RightCommitId=%q Paths=%s",
		repoPath,
		leftSha,
		rightSha,
		paths,
	)

	cmdArgs := []string{
		"--git-dir", repoPath,
		"diff",
		"--raw",
		"--abbrev=40",
		"--full-index",
		"--find-renames",
		leftSha,
		rightSha,
	}
	if len(paths) > 0 {
		cmdArgs = append(cmdArgs, "--")
		for _, path := range paths {
			cmdArgs = append(cmdArgs, string(path))
		}
	}

	var batch []*pb.CommitDelta
	var batchSize int

	flushFunc := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := stream.Send(&pb.CommitDeltaResponse{Deltas: batch}); err != nil {
			return grpc.Errorf(codes.Unavailable, "CommitDelta: send: %v", err)
		}

		return nil
	}

	err = eachDiff("CommitDelta", cmdArgs, func(diff *diff.Diff) error {
		delta := &pb.CommitDelta{
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

func eachDiff(rpc string, cmdArgs []string, callback func(*diff.Diff) error) error {
	cmd, err := helper.GitCommandReader(cmdArgs...)
	if err != nil {
		return grpc.Errorf(codes.Internal, "%s: cmd: %v", rpc, err)
	}
	defer cmd.Kill()

	diffParser := diff.NewDiffParser(cmd)

	for diffParser.Parse() {
		if err := callback(diffParser.Diff()); err != nil {
			return err
		}
	}

	if err := diffParser.Err(); err != nil {
		return grpc.Errorf(codes.Internal, "%s: parse failure: %v", rpc, err)
	}

	if err := cmd.Wait(); err != nil {
		return grpc.Errorf(codes.Unavailable, "%s: cmd wait for %v: %v", rpc, cmd.Args, err)
	}

	return nil
}

func deltaSize(diff *diff.Diff) int {
	size := len(diff.FromID) + len(diff.ToID) +
		4 + 4 + // OldMode and NewMode are int32 = 32/8 = 4 bytes
		len(diff.FromPath) + len(diff.ToPath)

	return size
}
