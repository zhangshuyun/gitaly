package commit

import (
	"fmt"
	"io"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) RawBlame(in *gitalypb.RawBlameRequest, stream gitalypb.CommitService_RawBlameServer) error {
	if err := validateRawBlameRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "RawBlame: %v", err)
	}

	ctx := stream.Context()
	revision := string(in.GetRevision())
	path := string(in.GetPath())

	cmd, err := git.SafeCmd(ctx, in.Repository, nil, git.SubCmd{
		Name:        "blame",
		Flags:       []git.Option{git.Flag{"-p"}},
		Args:        []string{revision},
		PostSepArgs: []string{path},
	})
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "RawBlame: cmd: %v", err)
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawBlameResponse{Data: p})
	})

	_, err = io.Copy(sw, cmd)
	if err != nil {
		return status.Errorf(codes.Unavailable, "RawBlame: send: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Info("ignoring git-blame error")
	}

	return nil
}

func validateRawBlameRequest(in *gitalypb.RawBlameRequest) error {
	if err := git.ValidateRevision(in.Revision); err != nil {
		return err
	}

	if len(in.GetPath()) == 0 {
		return fmt.Errorf("empty Path")
	}

	return nil
}
