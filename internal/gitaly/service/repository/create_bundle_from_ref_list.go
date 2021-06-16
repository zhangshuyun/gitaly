package repository

import (
	"bytes"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CreateBundleFromRefList(stream gitalypb.RepositoryService_CreateBundleFromRefListServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	if firstRequest.GetRepository() == nil {
		return status.Errorf(codes.InvalidArgument, "empty Repository")
	}

	ctx := stream.Context()

	if _, err := s.Cleanup(ctx, &gitalypb.CleanupRequest{Repository: firstRequest.GetRepository()}); err != nil {
		return err
	}

	firstRead := true
	reader := streamio.NewReader(func() ([]byte, error) {
		if firstRead {
			firstRead = false
			return []byte(fmt.Sprintln(firstRequest.GetPattern())), nil
		}

		request, err := stream.Recv()
		return []byte(fmt.Sprintln(request.GetPattern())), err
	})

	var stderr bytes.Buffer

	repo := s.localrepo(firstRequest.GetRepository())
	cmd, err := repo.Exec(ctx,
		git.SubSubCmd{
			Name:   "bundle",
			Action: "create",
			Flags:  []git.Option{git.OutputToStdout, git.Flag{Name: "--stdin"}},
		},
		git.WithStdin(reader),
		git.WithStderr(&stderr),
	)
	if err != nil {
		return status.Errorf(codes.Internal, "cmd start failed: %v", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.CreateBundleFromRefListResponse{Data: p})
	})

	_, err = io.Copy(writer, cmd)
	if err != nil {
		return status.Errorf(codes.Internal, "stream writer failed: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Internal, "cmd wait failed: %v, stderr: %q", err, stderr.String())
	}

	return nil
}
