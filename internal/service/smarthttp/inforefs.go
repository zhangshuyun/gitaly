package smarthttp

import (
	"context"
	"fmt"
	"io"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/diskcache"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/internal/global"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	uploadPack = "upload-pack"
)

// InfoRefsUploadPack is called when user is fetching info-refs over HTTP
func (s *server) InfoRefsUploadPack(in *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsUploadPackServer) error {
	w := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.InfoRefsResponse{Data: p})
	})

	db, err := global.DiskcacheDB()
	if err != nil {
		withServiceLogger(stream.Context(), uploadPack).Warnf("disk cache DB unavailable: %s", err)
		return handleInfoRefs(stream.Context(), uploadPack, in, w)
	}

	src, err := db.GetStream(in.GetRepository().GetRelativePath(), uploadPack)
	switch err {
	case nil:
		withServiceLogger(stream.Context(), uploadPack).Info("cache hit")
		_, err := io.Copy(w, src)
		return helper.ErrInternal(err)

	case diskcache.ErrNamespaceNotFound, diskcache.ErrKeyNotFound:
		withServiceLogger(stream.Context(), uploadPack).Info("cache miss")
		pr, pw := io.Pipe()
		defer pw.Close()

		go func() {
			err := db.PutStream(in.GetRepository().GetRelativePath(), uploadPack, pr)
			if err != nil {
				withServiceLogger(stream.Context(), uploadPack).Warnf("unable to cache stream: %s", err)
			}
		}()

		return handleInfoRefs(stream.Context(), uploadPack, in, pw)

	default:
		withServiceLogger(stream.Context(), uploadPack).Infof("cache error: %s", err)
		return handleInfoRefs(stream.Context(), uploadPack, in, w)
	}
}

func (s *server) InfoRefsReceivePack(in *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsReceivePackServer) error {
	w := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.InfoRefsResponse{Data: p})
	})
	return handleInfoRefs(stream.Context(), "receive-pack", in, w)
}

func withServiceLogger(ctx context.Context, service string) *log.Entry {
	return grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"service": service,
	})
}

func handleInfoRefs(ctx context.Context, service string, req *gitalypb.InfoRefsRequest, w io.Writer) error {
	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"service": service,
	}).Debug("handleInfoRefs")

	env := git.AddGitProtocolEnv(ctx, req, []string{})

	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	args := []string{}
	for _, params := range req.GitConfigOptions {
		args = append(args, "-c", params)
	}

	args = append(args, service, "--stateless-rpc", "--advertise-refs", repoPath)

	cmd, err := git.BareCommand(ctx, nil, nil, nil, env, args...)

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
