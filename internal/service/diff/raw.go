package diff

import (
	"io"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) RawDiff(in *gitalypb.RawDiffRequest, stream gitalypb.DiffService_RawDiffServer) error {
	if err := validateRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "RawDiff: %v", err)
	}

	cmdArgs := []string{"diff", "--full-index", in.LeftCommitId, in.RightCommitId}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawDiffResponse{Data: p})
	})

	return sendRawOutput(stream.Context(), "RawDiff", in.Repository, sw, cmdArgs)
}

func (s *server) RawPatch(in *gitalypb.RawPatchRequest, stream gitalypb.DiffService_RawPatchServer) error {
	if err := validateRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "RawPatch: %v", err)
	}

	cmdArgs := []string{"format-patch", "--stdout", in.LeftCommitId + ".." + in.RightCommitId}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawPatchResponse{Data: p})
	})

	return sendRawOutput(stream.Context(), "RawPatch", in.Repository, sw, cmdArgs)
}

func sendRawOutput(ctx context.Context, rpc string, repo *gitalypb.Repository, sender io.Writer, cmdArgs []string) error {
	cmd, err := git.Command(ctx, repo, cmdArgs...)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "%s: cmd: %v", rpc, err)
	}

	if _, err := io.Copy(sender, cmd); err != nil {
		return status.Errorf(codes.Unavailable, "%s: send: %v", rpc, err)
	}

	return cmd.Wait()
}
