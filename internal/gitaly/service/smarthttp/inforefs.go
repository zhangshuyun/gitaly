package smarthttp

import (
	"context"
	"fmt"
	"io"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	uploadPackSvc  = "upload-pack"
	receivePackSvc = "receive-pack"
)

func (s *server) InfoRefsUploadPack(in *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsUploadPackServer) error {
	w := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.InfoRefsResponse{Data: p})
	})

	return tryCache(stream.Context(), in, w, func(w io.Writer) error {
		return s.handleInfoRefs(stream.Context(), uploadPackSvc, in, w)
	})
}

func (s *server) InfoRefsReceivePack(in *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsReceivePackServer) error {
	w := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.InfoRefsResponse{Data: p})
	})
	return s.handleInfoRefs(stream.Context(), receivePackSvc, in, w)
}

func (s *server) handleInfoRefs(ctx context.Context, service string, req *gitalypb.InfoRefsRequest, w io.Writer) error {
	ctxlogrus.Extract(ctx).WithFields(log.Fields{
		"service": service,
	}).Debug("handleInfoRefs")

	repoPath, err := s.locator.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	cmdOpts := []git.CmdOpt{git.WithGitProtocol(ctx, req)}
	if service == "receive-pack" {
		cmdOpts = append(cmdOpts, git.WithRefTxHook(ctx, req.Repository, config.Config))
	}

	globalOpts := make([]git.GlobalOption, len(req.GitConfigOptions))
	for i, o := range req.GitConfigOptions {
		globalOpts[i] = git.ValueFlag{"-c", o}
	}

	cmd, err := git.NewCommandWithoutRepo(ctx, globalOpts, git.SubCmd{
		Name:  service,
		Flags: []git.Option{git.Flag{Name: "--stateless-rpc"}, git.Flag{Name: "--advertise-refs"}},
		Args:  []string{repoPath},
	}, cmdOpts...)

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
