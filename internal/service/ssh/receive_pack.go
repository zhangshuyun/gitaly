package ssh

import (
	"fmt"
	"os/exec"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) SSHReceivePack(stream gitalypb.SSHService_SSHReceivePackServer) error {
	ctx := stream.Context()
	req, err := stream.Recv() // First request contains only Repository, GlId, and GlUsername
	if err != nil {
		return err
	}

	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"GlID":             req.GlId,
		"GlRepository":     req.GlRepository,
		"GlUsername":       req.GlUsername,
		"GitConfigOptions": req.GitConfigOptions,
	}).Debug("SSHReceivePack")

	if err = validateFirstReceivePackRequest(req); err != nil {
		return err
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		request, err := stream.Recv()
		return request.GetStdin(), err
	})
	stdout := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.SSHReceivePackResponse{Stdout: p})
	})
	stderr := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.SSHReceivePackResponse{Stderr: p})
	})
	env := []string{
		fmt.Sprintf("GL_ID=%s", req.GlId),
		fmt.Sprintf("GL_USERNAME=%s", req.GlUsername),
		"GL_PROTOCOL=ssh",
	}
	if req.GlRepository != "" {
		env = append(env, fmt.Sprintf("GL_REPOSITORY=%s", req.GlRepository))
	}
	env = git.AddGitProtocolEnv(ctx, req, env)

	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	gitOptions := git.BuildGitOptions(req.GitConfigOptions, "receive-pack", repoPath)
	osCommand := exec.Command(command.GitPath(), gitOptions...)
	cmd, err := command.New(ctx, osCommand, stdin, stdout, stderr, env...)

	if err != nil {
		return status.Errorf(codes.Unavailable, "SSHReceivePack: cmd: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		if status, ok := command.ExitStatus(err); ok {
			return helper.DecorateError(
				codes.Internal,
				stream.Send(&gitalypb.SSHReceivePackResponse{ExitStatus: &gitalypb.ExitStatus{Value: int32(status)}}),
			)
		}
		return status.Errorf(codes.Unavailable, "SSHReceivePack: %v", err)
	}

	return nil
}

func validateFirstReceivePackRequest(req *gitalypb.SSHReceivePackRequest) error {
	if req.GlId == "" {
		return status.Errorf(codes.InvalidArgument, "SSHReceivePack: empty GlId")
	}
	if req.Stdin != nil {
		return status.Errorf(codes.InvalidArgument, "SSHReceivePack: non-empty data")
	}

	return nil
}
