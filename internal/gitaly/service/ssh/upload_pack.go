package ssh

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/stream"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
)

func (s *server) SSHUploadPack(stream gitalypb.SSHService_SSHUploadPackServer) error {
	ctx := stream.Context()

	req, err := stream.Recv() // First request contains Repository only
	if err != nil {
		return helper.ErrInternal(err)
	}

	repository := ""
	if req.Repository != nil {
		repository = req.Repository.GlRepository
	}

	ctxlogrus.Extract(ctx).WithFields(log.Fields{
		"GlRepository":     repository,
		"GitConfigOptions": req.GitConfigOptions,
		"GitProtocol":      req.GitProtocol,
	}).Debug("SSHUploadPack")

	if err = validateFirstUploadPackRequest(req); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		request, err := stream.Recv()
		return request.GetStdin(), err
	})

	// gRPC doesn't allow concurrent writes to a stream, so we need to
	// synchronize writing stdout and stderrr.
	var m sync.Mutex
	stdout := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.SSHUploadPackResponse{Stdout: p})
	})
	stderr := streamio.NewSyncWriter(&m, func(p []byte) error {
		return stream.Send(&gitalypb.SSHUploadPackResponse{Stderr: p})
	})

	if status, err := s.sshUploadPack(ctx, req, stdin, stdout, stderr); err != nil {
		if errSend := stream.Send(&gitalypb.SSHUploadPackResponse{
			ExitStatus: &gitalypb.ExitStatus{Value: int32(status)},
		}); errSend != nil {
			ctxlogrus.Extract(ctx).WithError(errSend).Error("send final status code")
		}

		return helper.ErrInternal(err)
	}

	return nil
}

type sshUploadPackRequest interface {
	GetRepository() *gitalypb.Repository
	GetGitConfigOptions() []string
	GetGitProtocol() string
}

func (s *server) sshUploadPack(ctx context.Context, req sshUploadPackRequest, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	stdoutCounter := &helper.CountingWriter{W: stdout}
	// Use large copy buffer to reduce the number of system calls
	stdout = &largeBufferReaderFrom{Writer: stdoutCounter}

	repo := req.GetRepository()
	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return 0, err
	}

	git.WarnIfTooManyBitmaps(ctx, s.locator, repo.StorageName, repoPath)

	config, err := git.ConvertConfigOptions(req.GetGitConfigOptions())
	if err != nil {
		return 0, err
	}

	pr, pw := io.Pipe()
	defer pw.Close()
	stdin = io.TeeReader(stdin, pw)
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
			pr.Close()
		}()

		stats, err := stats.ParsePackfileNegotiation(pr)
		if err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Debug("failed parsing packfile negotiation")
			return
		}
		stats.UpdateMetrics(s.packfileNegotiationMetrics)
	}()

	commandOpts := []git.CmdOpt{
		git.WithGitProtocol(req),
		git.WithConfig(config...),
		git.WithPackObjectsHookEnv(repo),
	}

	var stderrBuilder strings.Builder
	stderr = io.MultiWriter(stderr, &stderrBuilder)

	cmd, monitor, err := monitorStdinCommand(ctx, s.gitCmdFactory, stdin, stdout, stderr, git.SubCmd{
		Name: "upload-pack",
		Args: []string{repoPath},
	}, commandOpts...)
	if err != nil {
		return 0, err
	}

	timeoutTicker := helper.NewTimerTicker(s.uploadPackRequestTimeout)

	// upload-pack negotiation is terminated by either a flush, or the "done"
	// packet: https://github.com/git/git/blob/v2.20.0/Documentation/technical/pack-protocol.txt#L335
	//
	// "flush" tells the server it can terminate, while "done" tells it to start
	// generating a packfile. Add a timeout to the second case to mitigate
	// use-after-check attacks.
	go monitor.Monitor(ctx, pktline.PktDone(), timeoutTicker, cancelCtx)

	if err := cmd.Wait(); err != nil {
		pw.Close()
		wg.Wait()

		status, _ := command.ExitStatus(err)
		return status, fmt.Errorf("cmd wait: %w, stderr: %q", err, stderrBuilder.String())
	}

	pw.Close()
	wg.Wait()

	ctxlogrus.Extract(ctx).WithField("response_bytes", stdoutCounter.N).Info("request details")

	return 0, nil
}

func validateFirstUploadPackRequest(req *gitalypb.SSHUploadPackRequest) error {
	if req.Stdin != nil {
		return fmt.Errorf("non-empty stdin in first request")
	}

	return nil
}

type largeBufferReaderFrom struct {
	io.Writer
}

func (rf *largeBufferReaderFrom) ReadFrom(r io.Reader) (int64, error) {
	return io.CopyBuffer(rf.Writer, r, make([]byte, 64*1024))
}

func (s *server) SSHUploadPackWithSidechannel(ctx context.Context, req *gitalypb.SSHUploadPackWithSidechannelRequest) (*gitalypb.SSHUploadPackWithSidechannelResponse, error) {
	conn, err := sidechannel.OpenSidechannel(ctx)
	if err != nil {
		return nil, helper.ErrUnavailable(err)
	}
	defer conn.Close()

	sidebandWriter := pktline.NewSidebandWriter(conn)
	stdout := sidebandWriter.Writer(stream.BandStdout)
	stderr := sidebandWriter.Writer(stream.BandStderr)
	if _, err := s.sshUploadPack(ctx, req, conn, stdout, stderr); err != nil {
		return nil, helper.ErrInternal(err)
	}
	if err := conn.Close(); err != nil {
		return nil, helper.ErrInternalf("close sidechannel: %w", err)
	}

	return &gitalypb.SSHUploadPackWithSidechannelResponse{}, nil
}
