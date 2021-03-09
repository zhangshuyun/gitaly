package ssh

import (
	"errors"
	"fmt"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (s *server) SSHReceivePack(stream gitalypb.SSHService_SSHReceivePackServer) error {
	req, err := stream.Recv() // First request contains only Repository, GlId, and GlUsername
	if err != nil {
		return helper.ErrInternal(err)
	}

	ctxlogrus.Extract(stream.Context()).WithFields(log.Fields{
		"GlID":             req.GlId,
		"GlRepository":     req.GlRepository,
		"GlUsername":       req.GlUsername,
		"GitConfigOptions": req.GitConfigOptions,
		"GitProtocol":      req.GitProtocol,
	}).Debug("SSHReceivePack")

	if err = validateFirstReceivePackRequest(req); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := s.sshReceivePack(stream, req); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) sshReceivePack(stream gitalypb.SSHService_SSHReceivePackServer, req *gitalypb.SSHReceivePackRequest) error {
	ctx := stream.Context()

	stdin := streamio.NewReader(func() ([]byte, error) {
		request, err := stream.Recv()
		return request.GetStdin(), err
	})

	var m sync.Mutex
	stdout := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.SSHReceivePackResponse{Stdout: p})
	})
	stderr := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.SSHReceivePackResponse{Stderr: p})
	})

	repoPath, err := s.locator.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	config, err := git.ConvertConfigOptions(req.GitConfigOptions)
	if err != nil {
		return err
	}

	cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
		git.SubCmd{
			Name: "receive-pack",
			Args: []string{repoPath},
		},
		git.WithStdin(stdin),
		git.WithStdout(stdout),
		git.WithStderr(stderr),
		git.WithReceivePackHooks(ctx, s.cfg, req, "ssh"),
		git.WithGitProtocol(ctx, req),
		git.WithConfig(config...),
	)

	if err != nil {
		return fmt.Errorf("start cmd: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		if status, ok := command.ExitStatus(err); ok {
			return stream.Send(&gitalypb.SSHReceivePackResponse{
				ExitStatus: &gitalypb.ExitStatus{Value: int32(status)},
			})
		}

		return fmt.Errorf("cmd wait: %v", err)
	}

	return nil
}

func validateFirstReceivePackRequest(req *gitalypb.SSHReceivePackRequest) error {
	if req.GlId == "" {
		return errors.New("empty GlId")
	}
	if req.Stdin != nil {
		return errors.New("non-empty data in first request")
	}
	if req.Repository == nil {
		return errors.New("repository is empty")
	}

	return nil
}
