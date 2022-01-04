package smarthttp

import (
	"context"
	"crypto/sha1"
	"fmt"
	"io"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type basicPostUploadPackRequest interface {
	GetRepository() *gitalypb.Repository
	GetGitConfigOptions() []string
	GetGitProtocol() string
}

func (s *server) PostUploadPack(stream gitalypb.SmartHTTPService_PostUploadPackServer) error {
	ctx := stream.Context()

	req, err := stream.Recv() // First request contains Repository only
	if err != nil {
		return err
	}

	if req.Data != nil {
		return status.Errorf(codes.InvalidArgument, "non-empty Data")
	}

	repoPath, gitConfig, err := s.validateUploadPackRequest(ctx, req)
	if err != nil {
		return err
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()

		return resp.GetData(), err
	})
	stdout := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.PostUploadPackResponse{Data: p})
	})

	return s.runUploadPack(ctx, req, repoPath, gitConfig, stdin, stdout)
}

func (s *server) PostUploadPackWithSidechannel(ctx context.Context, req *gitalypb.PostUploadPackWithSidechannelRequest) (*gitalypb.PostUploadPackWithSidechannelResponse, error) {
	repoPath, gitConfig, err := s.validateUploadPackRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	conn, err := sidechannel.OpenSidechannel(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open sidechannel: %v", err)
	}
	defer conn.Close()

	if err := s.runUploadPack(ctx, req, repoPath, gitConfig, conn, conn); err != nil {
		return nil, err
	}

	if err := conn.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "close sidechannel connection: %v", err)
	}

	return &gitalypb.PostUploadPackWithSidechannelResponse{}, nil
}

type statsCollector struct {
	c       io.Closer
	statsCh chan stats.PackfileNegotiation
}

func (sc *statsCollector) finish() stats.PackfileNegotiation {
	sc.c.Close()
	return <-sc.statsCh
}

func (s *server) runStatsCollector(ctx context.Context, r io.Reader) (io.Reader, *statsCollector) {
	pr, pw := io.Pipe()
	sc := &statsCollector{
		c:       pw,
		statsCh: make(chan stats.PackfileNegotiation, 1),
	}

	go func() {
		defer close(sc.statsCh)

		stats, err := stats.ParsePackfileNegotiation(pr)
		if err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Debug("failed parsing packfile negotiation")
			return
		}
		stats.UpdateMetrics(s.packfileNegotiationMetrics)

		sc.statsCh <- stats
	}()

	return io.TeeReader(r, pw), sc
}

func (s *server) validateUploadPackRequest(ctx context.Context, req basicPostUploadPackRequest) (string, []git.ConfigPair, error) {
	repoPath, err := s.locator.GetRepoPath(req.GetRepository())
	if err != nil {
		return "", nil, helper.ErrInvalidArgument(err)
	}

	git.WarnIfTooManyBitmaps(ctx, s.locator, req.GetRepository().GetStorageName(), repoPath)

	config, err := git.ConvertConfigOptions(req.GetGitConfigOptions())
	if err != nil {
		return "", nil, helper.ErrInvalidArgument(err)
	}

	return repoPath, config, nil
}

func (s *server) runUploadPack(ctx context.Context, req basicPostUploadPackRequest, repoPath string, gitConfig []git.ConfigPair, stdin io.Reader, stdout io.Writer) error {
	h := sha1.New()

	stdin = io.TeeReader(stdin, h)
	stdin, collector := s.runStatsCollector(ctx, stdin)
	defer collector.finish()

	commandOpts := []git.CmdOpt{
		git.WithStdin(stdin),
		git.WithGitProtocol(ctx, req),
		git.WithConfig(gitConfig...),
		git.WithPackObjectsHookEnv(ctx, req.GetRepository(), s.cfg),
	}

	cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx, git.SubCmd{
		Name:  "upload-pack",
		Flags: []git.Option{git.Flag{Name: "--stateless-rpc"}},
		Args:  []string{repoPath},
	}, commandOpts...)
	if err != nil {
		return helper.ErrUnavailablef("cmd: %v", err)
	}

	// Use a custom buffer size to minimize the number of system calls.
	respBytes, err := io.CopyBuffer(stdout, cmd, make([]byte, 64*1024))
	if err != nil {
		return helper.ErrUnavailablef("Fail to transfer git data: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		stats := collector.finish()

		if _, ok := command.ExitStatus(err); ok && stats.Deepen != "" {
			// We have seen a 'deepen' message in the request. It is expected that
			// git-upload-pack has a non-zero exit status: don't treat this as an
			// error.
			return nil
		}

		return helper.ErrUnavailable(err)
	}

	ctxlogrus.Extract(ctx).WithField("request_sha", fmt.Sprintf("%x", h.Sum(nil))).WithField("response_bytes", respBytes).Info("request details")

	return nil
}
