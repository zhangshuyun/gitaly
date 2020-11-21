package hook

import (
	"os/exec"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (s *server) PackObjectsHook(stream gitalypb.HookService_PackObjectsHookServer) error {
	ctx := stream.Context()

	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternal(err)
	}

	repoPath, err := helper.GetRepoPath(firstRequest.Repository)
	if err != nil {
		return err
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	})

	var m sync.Mutex
	stdout := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.PackObjectsHookResponse{Stdout: p})
	})
	stderr := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.PackObjectsHookResponse{Stderr: p})
	})

	cmd, err := command.New(ctx, exec.Command("git", append([]string{"-C", repoPath}, firstRequest.Args...)...), stdin, stdout, stderr)
	if err != nil {
		return err
	}
	lastResponse := &gitalypb.PackObjectsHookResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: 0},
	}
	if err := cmd.Wait(); err != nil {
		lastResponse.ExitStatus.Value = 1
	}

	return stream.Send(lastResponse)

}
