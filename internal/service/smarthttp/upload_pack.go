package smarthttp

import (
	"log"

	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type uploadPackBytesReader struct {
	pb.SmartHTTP_PostUploadPackServer
}

type uploadPackWriter struct {
	pb.SmartHTTP_PostUploadPackServer
}

func (s *server) PostUploadPack(stream pb.SmartHTTP_PostUploadPackServer) error {
	req, err := stream.Recv() // First request contains Repository only
	if err != nil {
		return err
	}
	if err := validateUploadPackRequest(req); err != nil {
		return err
	}

	streamBytesReader := uploadPackBytesReader{stream}
	stdin := &streamReader{br: streamBytesReader}
	stdout := uploadPackWriter{stream}
	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	log.Printf("PostUploadPack: RepoPath=%q", repoPath)

	osCommand := helper.CommandWrapper(stream.Context(), "git", "upload-pack", "--stateless-rpc", repoPath)
	cmd, err := helper.NewCommand(osCommand, stdin, stdout)

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

func (rw uploadPackWriter) Write(p []byte) (int, error) {
	resp := &pb.PostUploadPackResponse{Data: p}
	if err := rw.Send(resp); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (br uploadPackBytesReader) ReceiveBytes() ([]byte, error) {
	resp, err := br.Recv()
	if err != nil {
		return nil, err
	}

	return resp.GetData(), nil
}
