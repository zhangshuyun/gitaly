package smarthttp

import (
	"context"
	"fmt"
	"io"
	"os/exec"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) InfoRefsUploadPack(in *pb.InfoRefsRequest, stream pb.SmartHTTPService_InfoRefsUploadPackServer) error {
	w := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&pb.InfoRefsResponse{Data: p})
	})
	return handleInfoRefs(stream.Context(), "upload-pack", in, w)
}

func (s *server) InfoRefsReceivePack(in *pb.InfoRefsRequest, stream pb.SmartHTTPService_InfoRefsReceivePackServer) error {
	w := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&pb.InfoRefsResponse{Data: p})
	})
	return handleInfoRefs(stream.Context(), "receive-pack", in, w)
}

func handleInfoRefs(ctx context.Context, service string, req *pb.InfoRefsRequest, w io.Writer) error {
	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"service":     service,
		"GitProtocol": req.GitProtocol,
	}).Debug("handleInfoRefs")

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

	args = append(args, service, "--stateless-rpc", "--advertise-refs", repoPath)

	osCommand := exec.Command(command.GitPath(), args...)
	cmd, err := command.New(ctx, osCommand, nil, nil, nil, env...)

	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "GetInfoRefs: cmd: %v", err)
	}

	if _, err := pktline.WriteString(w, fmt.Sprintf("# service=git-%s\n", service)); err != nil {
		return status.Errorf(codes.Internal, "GetInfoRefs: pktLine: %v", err)
	}

	if err := pktline.WriteFlush(w); err != nil {
		return status.Errorf(codes.Internal, "GetInfoRefs: pktFlush: %v", err)
	}

	if _, err := io.Copy(w, cmd); err != nil {
		return status.Errorf(codes.Internal, "GetInfoRefs: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Internal, "GetInfoRefs: %v", err)
	}

	return nil
}
