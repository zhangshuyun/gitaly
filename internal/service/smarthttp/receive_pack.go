package smarthttp

import (
	"fmt"
	"strconv"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) PostReceivePack(stream gitalypb.SmartHTTPService_PostReceivePackServer) error {
	ctx := stream.Context()
	req, err := stream.Recv() // First request contains only Repository and GlId
	if err != nil {
		return err
	}

	ctxlogrus.Extract(ctx).WithFields(log.Fields{
		"GlID":             req.GlId,
		"GlRepository":     req.GlRepository,
		"GlUsername":       req.GlUsername,
		"GitConfigOptions": req.GitConfigOptions,
	}).Debug("PostReceivePack")

	if err := validateReceivePackRequest(req); err != nil {
		return err
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	stdout := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.PostReceivePackResponse{Data: p})
	})

	hookEnv, err := git.HookEnv(req)
	if err != nil {
		return err
	}
	env := append(hookEnv, "GL_PROTOCOL=http")

	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	env = git.AddGitProtocolEnv(ctx, req, env)
	env = append(env, command.GitEnv...)
	env = append(env, fmt.Sprintf("%s=%s", featureflag.HooksRPCEnvVar, strconv.FormatBool(featureflag.IsEnabled(ctx, featureflag.HooksRPC))))

	globalOpts := git.ReceivePackConfig()
	for _, o := range req.GitConfigOptions {
		globalOpts = append(globalOpts, git.ValueFlag{"-c", o})
	}

	cmd, err := git.SafeBareCmd(ctx, stdin, stdout, nil, env, globalOpts, git.SubCmd{
		Name:  "receive-pack",
		Flags: []git.Option{git.Flag{"--stateless-rpc"}},
		Args:  []string{repoPath},
	})

	if err != nil {
		return status.Errorf(codes.Unavailable, "PostReceivePack: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Unavailable, "PostReceivePack: %v", err)
	}

	return nil
}

func validateReceivePackRequest(req *gitalypb.PostReceivePackRequest) error {
	if req.GlId == "" {
		return status.Errorf(codes.InvalidArgument, "PostReceivePack: empty GlId")
	}
	if req.Data != nil {
		return status.Errorf(codes.InvalidArgument, "PostReceivePack: non-empty Data")
	}
	if req.Repository == nil {
		return helper.ErrInvalidArgumentf("PostReceivePack: empty Repository")
	}

	return nil
}
