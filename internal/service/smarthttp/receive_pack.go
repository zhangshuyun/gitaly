package smarthttp

import (
	"fmt"
	"log"

	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type receivePackBytesReader struct {
	pb.SmartHTTP_PostReceivePackServer
}

type receivePackWriter struct {
	pb.SmartHTTP_PostReceivePackServer
}

func (s *server) PostReceivePack(stream pb.SmartHTTP_PostReceivePackServer) error {
	req, err := stream.Recv() // First request contains only Repository and GlId
	if err != nil {
		return err
	}
	if err := validateReceivePackRequest(req); err != nil {
		return err
	}

	streamBytesReader := receivePackBytesReader{stream}
	stdin := &streamReader{br: streamBytesReader}
	stdout := receivePackWriter{stream}
	glIDEnv := fmt.Sprintf("GL_ID=%s", req.GlId)
	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	log.Printf("PostReceivePack: RepoPath=%q GlID=%q", repoPath, req.GlId)

	osCommand := helper.CommandWrapper(stream.Context(), "git", "receive-pack", "--stateless-rpc", repoPath)
	cmd, err := helper.NewCommand(osCommand, stdin, stdout, glIDEnv)

	if err != nil {
		return grpc.Errorf(codes.Unavailable, "PostReceivePack: cmd: %v", err)
	}
	defer cmd.Kill()

	if err := cmd.Wait(); err != nil {
		return grpc.Errorf(codes.Unavailable, "PostReceivePack: cmd wait for %v: %v", cmd.Args, err)
	}

	return nil
}

func validateReceivePackRequest(req *pb.PostReceivePackRequest) error {
	if req.GlId == "" {
		return grpc.Errorf(codes.InvalidArgument, "PostReceivePack: empty GlId")
	}
	if req.Data != nil {
		return grpc.Errorf(codes.InvalidArgument, "PostReceivePack: non-empty Data")
	}

	return nil
}

func (rw receivePackWriter) Write(p []byte) (int, error) {
	resp := &pb.PostReceivePackResponse{Data: p}
	if err := rw.Send(resp); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (br receivePackBytesReader) ReceiveBytes() ([]byte, error) {
	resp, err := br.Recv()
	if err != nil {
		return nil, err
	}

	return resp.GetData(), nil
}
