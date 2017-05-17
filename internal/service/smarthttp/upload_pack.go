package smarthttp

import (
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	pbhelper "gitlab.com/gitlab-org/gitaly-proto/go/helper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) PostUploadPack(stream pb.SmartHTTP_PostUploadPackServer) error {
	req, err := stream.Recv() // First request contains Repository only
	if err != nil {
		return err
	}
	if err := validateUploadPackRequest(req); err != nil {
		return err
	}

	stdin := pbhelper.NewReceiveReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	stdout := pbhelper.NewSendWriter(func(p []byte) error {
		return stream.Send(&pb.PostUploadPackResponse{Data: p})
	})
	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	helper.Debugf("PostUploadPack: RepoPath=%q", repoPath)

	osCommand := exec.Command("git", "upload-pack", "--stateless-rpc", repoPath)
	cmd, err := helper.NewCommand(osCommand, stdin, stdout, nil)

	if err != nil {
		return grpc.Errorf(codes.Unavailable, "PostUploadPack: cmd: %v", err)
	}
	defer cmd.Kill()

	if err := cmd.Wait(); err != nil {
		return grpc.Errorf(codes.Unavailable, "PostUploadPack: cmd wait for %v: %v", cmd.Args, err)
	}

	return nil
}

func validateUploadPackRequest(req *pb.PostUploadPackRequest) error {
	if req.Data != nil {
		return grpc.Errorf(codes.InvalidArgument, "PostUploadPack: non-empty Data")
	}

	return nil
}
