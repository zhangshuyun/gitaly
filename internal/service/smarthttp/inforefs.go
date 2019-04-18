package smarthttp

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func gitConfigOptions(req *gitalypb.InfoRefsRequest) []string {
	args := []string{}
	for _, params := range req.GitConfigOptions {
		args = append(args, "-c", params)
	}
	return args
}

func getConfig(ctx context.Context, req *gitalypb.InfoRefsRequest, key string) (string, error) {
	args := gitConfigOptions(req)
	args = append(args, "config", "core.logAllRefUpdates")
	cmd, err := git.Command(ctx, req.Repository, args...)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return "", err
		}
		return "", status.Errorf(codes.Internal, "getConfig: cmd: %v", err)
	}

	data, err := ioutil.ReadAll(cmd)
	if err != nil {
		return "", status.Errorf(codes.Internal, "getConfig: cmd: %v", err)
	}

	return string(data), cmd.Wait()
}

func supportsInfoRefsCaching(ctx context.Context, req *gitalypb.InfoRefsRequest) (bool, error) {
	configOption, err := getConfig(ctx, req, "core.logAllRefUpdates")
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return false, err
		}
		return false, status.Errorf(codes.Internal, "GetCachedInfoRefs: cmd: %v", err)
	}

	if configOption != "always" {
		return false, nil
	}

	return true, nil
}

func (s *server) InfoRefsUploadPack(in *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsUploadPackServer) error {
	w := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.InfoRefsResponse{Data: p})
	})

	return handleInfoRefs(stream.Context(), "upload-pack", in, w)
}

func (s *server) InfoRefsReceivePack(in *gitalypb.InfoRefsRequest, stream gitalypb.SmartHTTPService_InfoRefsReceivePackServer) error {
	w := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.InfoRefsResponse{Data: p})
	})
	return handleInfoRefs(stream.Context(), "receive-pack", in, w)
}

func readCachedInfoRefs(ctx context.Context, req *gitalypb.InfoRefsRequest, w io.Writer) error {
	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	// if logs/refs is present it means that we have stale cache
	_, err = os.Lstat(
		filepath.Join(repoPath, "logs", "refs"))
	if err == nil {
		return os.ErrNotExist
	} else if !os.IsNotExist(err) {
		return err
	}

	infoRefs, err := os.Open(
		filepath.Join(repoPath, "info-refs"))
	if err != nil {
		return err
	}
	defer infoRefs.Close()

	_, err = io.Copy(w, infoRefs)
	if err != nil {
		return err
	}
	return nil
}

func hasLogsRefs(ctx context.Context, req *gitalypb.InfoRefsRequest) (bool, error) {
	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return false, err
	}

	logsRefRepoPath := filepath.Join(repoPath, "logs", "refs")
	_, err = os.Lstat(logsRefRepoPath)

	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

func createCachedInfoRefs(ctx context.Context, service string, req *gitalypb.InfoRefsRequest, w io.Writer) error {
	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	// try to remove all existing log refs
	// we do not care about errors, as execute that on best efforr
	os.RemoveAll(filepath.Join(repoPath, "logs", "refs"))

	// create temporary file
	tmpPath := filepath.Join(repoPath, "tmp")
	tmpInfoLogs, err := ioutil.TempFile(tmpPath, "info-logs")
	if err != nil {
		return status.Errorf(codes.Internal, "CachedInfoRefs: %v", err)
	}
	defer tmpInfoLogs.Close()
	defer os.Remove(tmpInfoLogs.Name())

	// write to output, and to file
	multiWriter := io.MultiWriter(tmpInfoLogs, w)
	err = handleInfoRefs(ctx, service, req, multiWriter)
	if err != nil {
		return status.Errorf(codes.Internal, "CachedInfoRefs: %v", err)
	}
	tmpInfoLogs.Close()

	// we do not care about errors, as execute that on best effort
	infoRefsPath := filepath.Join(repoPath, "info-refs")
	os.Rename(tmpInfoLogs.Name(), infoRefsPath)
	return nil
}

func handleCachedInfoRefs(ctx context.Context, service string, req *gitalypb.InfoRefsRequest, w io.Writer) error {
	err := readCachedInfoRefs(ctx, req, w)
	if os.IsNotExist(err) {
		return createCachedInfoRefs(ctx, service, req, w)
	} else if err != nil {
		return status.Errorf(codes.Internal, "CachedInfoRefs: cmd: %v", err)
	}

	return nil
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

	args := gitConfigOptions(req)
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
