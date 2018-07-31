package ssh

import (
	"fmt"
	"os/exec"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) SSHUploadPack(stream pb.SSHService_SSHUploadPackServer) error {
	req, err := stream.Recv() // First request contains Repository only
	if err != nil {
		return err
	}

	grpc_logrus.Extract(stream.Context()).WithFields(log.Fields{
		"GitProtocol": req.GitProtocol,
	}).Debug("SSHUploadPack")

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

	env := []string{}

	if req.GitProtocol == "version=2" {
		env = append(env, fmt.Sprintf("GIT_PROTOCOL=%s", req.GitProtocol))
	}

	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	args := []string{}

	for _, params := range req.GitConfigOptions {
		args = append(args, "-c", params)
	}

	args = append(args, "upload-pack", repoPath)

	osCommand := exec.Command(command.GitPath(), args...)

	cmd, err := command.New(stream.Context(), osCommand, stdin, stdout, stderr, env...)

	if err != nil {
		return status.Errorf(codes.Unavailable, "SSHUploadPack: cmd: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		if status, ok := command.ExitStatus(err); ok {
			return helper.DecorateError(
				codes.Internal,
				stream.Send(&pb.SSHUploadPackResponse{ExitStatus: &pb.ExitStatus{Value: int32(status)}}),
			)
		}
		return status.Errorf(codes.Unavailable, "SSHUploadPack: %v", err)
	}

	return nil
}

func validateFirstUploadPackRequest(req *pb.SSHUploadPackRequest) error {
	if req.Stdin != nil {
		return status.Errorf(codes.InvalidArgument, "SSHUploadPack: non-empty stdin")
	}

	return nil
}
