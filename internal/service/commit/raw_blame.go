package commit

import (
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) RawBlame(in *pb.RawBlameRequest, stream pb.CommitService_RawBlameServer) error {
	if err := validateRawBlameRequest(in); err != nil {
		return grpc.Errorf(codes.InvalidArgument, "RawBlame: %v", err)
	}

	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}

	ctx := stream.Context()
	revision := string(in.GetRevision())
	path := string(in.GetPath())

	cmd, err := helper.GitCommandReader(ctx, "--git-dir", repoPath, "blame", "-p", revision, "--", path)
	if err != nil {
		return grpc.Errorf(codes.Internal, "RawBlame: cmd: %v", err)
	}
	defer cmd.Kill(ctx)

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&pb.RawBlameResponse{Data: p})
	})

	_, err = io.Copy(sw, cmd)
	if err != nil {
		return grpc.Errorf(codes.Unavailable, "RawBlame: send: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		grpc_logrus.Extract(ctx).WithError(err).Info("ignoring git-blame error")
	}

	return nil
}

func validateRawBlameRequest(in *pb.RawBlameRequest) error {
	if len(in.GetRevision()) == 0 {
		return fmt.Errorf("empty Revision")
	}

	if len(in.GetPath()) == 0 {
		return fmt.Errorf("empty Path")
	}

	return nil
}
