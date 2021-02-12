package diff

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) RawDiff(in *gitalypb.RawDiffRequest, stream gitalypb.DiffService_RawDiffServer) error {
	if err := validateRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "RawDiff: %v", err)
	}

	subCmd := git.SubCmd{
		Name:  "diff",
		Flags: []git.Option{git.Flag{Name: "--full-index"}},
		Args:  []string{in.LeftCommitId, in.RightCommitId},
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawDiffResponse{Data: p})
	})

	return sendRawOutput(stream.Context(), s.gitCmdFactory, "RawDiff", in.Repository, sw, subCmd)
}

func (s *server) RawPatch(in *gitalypb.RawPatchRequest, stream gitalypb.DiffService_RawPatchServer) error {
	if err := validateRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "RawPatch: %v", err)
	}

	subCmd := git.SubCmd{
		Name:  "format-patch",
		Flags: []git.Option{git.Flag{Name: "--stdout"}, git.ValueFlag{"--signature", "GitLab"}},
		Args:  []string{in.LeftCommitId + ".." + in.RightCommitId},
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.RawPatchResponse{Data: p})
	})

	return sendRawOutput(stream.Context(), s.gitCmdFactory, "RawPatch", in.Repository, sw, subCmd)
}

func sendRawOutput(ctx context.Context, gitCmdFactory git.CommandFactory, rpc string, repo *gitalypb.Repository, sender io.Writer, subCmd git.SubCmd) error {
	cmd, err := gitCmdFactory.New(ctx, repo, nil, subCmd)
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
