package ssh

import (
	"io"
	"os/exec"

	log "github.com/sirupsen/logrus"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	pbhelper "gitlab.com/gitlab-org/gitaly-proto/go/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) SSHUploadPack(stream pb.SSH_SSHUploadPackServer) error {
	req, err := stream.Recv() // First request contains Repository only
	if err != nil {
		return err
	}
	if err = validateFirstUploadPackRequest(req); err != nil {
		return err
	}

	var stdin io.Reader
	stdin = pbhelper.NewReceiveReader(func() ([]byte, error) {
		request, errRecv := stream.Recv()
		if len(request.Stdin) <= 0 {
			// NOTE: Nasty hack to close stdin
			return nil, io.EOF
		}
		return request.GetStdin(), errRecv
	})
	stdout := pbhelper.NewSendWriter(func(p []byte) error {
		return stream.Send(&pb.SSHUploadPackResponse{Stdout: p})
	})
	stderr := pbhelper.NewSendWriter(func(p []byte) error {
		return stream.Send(&pb.SSHUploadPackResponse{Stderr: p})
	})
	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"RepoPath": repoPath,
	}).Debug("SSHUploadPack")

	osCommand := exec.Command("git-upload-pack", repoPath)
	cmd, err := helper.NewCommand(osCommand, stdin, stdout, stderr)

	if err != nil {
		return grpc.Errorf(codes.Unavailable, "SSHUploadPack: cmd: %v", err)
	}
	defer cmd.Kill()

	if err := cmd.Wait(); err != nil {
		if status, ok := helper.ExitStatus(err); ok {

			return helper.DecorateError(
				codes.Internal,
				stream.Send(&pb.SSHUploadPackResponse{ExitStatus: &pb.ExitStatus{Value: int32(status)}}),
			)
		}
		return grpc.Errorf(codes.Unavailable, "SSHUploadPack: cmd wait for %v: %v", cmd.Args, err)
	}

	log.WithFields(log.Fields{
		"RepoPath": repoPath,
	}).Debug("SSHUploadPack Done")

	return helper.DecorateError(
		codes.Internal,
		stream.Send(&pb.SSHUploadPackResponse{ExitStatus: &pb.ExitStatus{Value: int32(0)}}),
	)
}

func validateFirstUploadPackRequest(req *pb.SSHUploadPackRequest) error {
	if req.Stdin != nil {
		return grpc.Errorf(codes.InvalidArgument, "SSHUploadPack: non-empty stdin")
	}

	return nil
}
