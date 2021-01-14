package ssh

import (
	"context"
	"fmt"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (s *server) SSHUploadArchive(stream gitalypb.SSHService_SSHUploadArchiveServer) error {
	req, err := stream.Recv() // First request contains Repository only
	if err != nil {
		return helper.ErrInternal(err)
	}
	if err = validateFirstUploadArchiveRequest(req); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err = s.sshUploadArchive(stream, req); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) sshUploadArchive(stream gitalypb.SSHService_SSHUploadArchiveServer, req *gitalypb.SSHUploadArchiveRequest) error {
	ctx, cancelCtx := context.WithCancel(stream.Context())
	defer cancelCtx()

	repoPath, err := s.locator.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		request, err := stream.Recv()
		return request.GetStdin(), err
	})

	var m sync.Mutex
	stdout := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.SSHUploadArchiveResponse{Stdout: p})
	})
	stderr := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.SSHUploadArchiveResponse{Stderr: p})
	})

	cmd, monitor, err := monitorStdinCommand(ctx, stdin, stdout, stderr, nil, git.SubCmd{
		Name: "upload-archive",
		Args: []string{repoPath},
	})
	if err != nil {
		return err
	}

	// upload-archive expects a list of options terminated by a flush packet:
	// https://github.com/git/git/blob/v2.22.0/builtin/upload-archive.c#L38
	//
	// Place a timeout on receiving the flush packet to mitigate use-after-check
	// attacks
	go monitor.Monitor(pktline.PktFlush(), s.uploadArchiveRequestTimeout, cancelCtx)

	if err := cmd.Wait(); err != nil {
		if status, ok := command.ExitStatus(err); ok {
			return stream.Send(&gitalypb.SSHUploadArchiveResponse{
				ExitStatus: &gitalypb.ExitStatus{Value: int32(status)},
			})
		}
		return fmt.Errorf("wait cmd: %v", err)
	}

	return stream.Send(&gitalypb.SSHUploadArchiveResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: 0},
	})
}

func validateFirstUploadArchiveRequest(req *gitalypb.SSHUploadArchiveRequest) error {
	if req.Stdin != nil {
		return fmt.Errorf("non-empty stdin in first request")
	}

	return nil
}
