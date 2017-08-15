package ssh

import (
	"os/exec"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) SSHUploadPack(stream pb.SSHService_SSHUploadPackServer) error {
	grpc_logrus.Extract(stream.Context()).Debug("SSHUploadPack")

	req, err := stream.Recv() // First request contains Repository only
	if err != nil {
		return err
	}
	if err = validateFirstUploadPackRequest(req); err != nil {
		return err
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		request, err := stream.Recv()
		return request.GetStdin(), err
	})
	stdout := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&pb.SSHUploadPackResponse{Stdout: p})
	})
	stderr := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&pb.SSHUploadPackResponse{Stderr: p})
	})
	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	args := []string{}

	for _, params := range req.GitConfigOptions {
		args = append(args, "-c", params)
	}

	args = append(args, "upload-pack", repoPath)

	osCommand := exec.Command(helper.GitPath(), args...)

	cmd, err := helper.NewCommand(stream.Context(), osCommand, stdin, stdout, stderr)

	if err != nil {
		return grpc.Errorf(codes.Unavailable, "SSHUploadPack: cmd: %v", err)
	}
	defer cmd.CleanUpProcessGroup(stream.Context())

	if err := cmd.Wait(); err != nil {
		if status, ok := helper.ExitStatus(err); ok {
			return helper.DecorateError(
				codes.Internal,
				stream.Send(&pb.SSHUploadPackResponse{ExitStatus: &pb.ExitStatus{Value: int32(status)}}),
			)
		}
		return grpc.Errorf(codes.Unavailable, "SSHUploadPack: cmd wait for %v: %v", cmd.Args, err)
	}

	return nil
}

func validateFirstUploadPackRequest(req *pb.SSHUploadPackRequest) error {
	if req.Stdin != nil {
		return grpc.Errorf(codes.InvalidArgument, "SSHUploadPack: non-empty stdin")
	}

	return nil
}
