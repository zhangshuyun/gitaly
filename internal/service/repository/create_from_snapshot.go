package repository

import (
	"net"
	"net/http"
	"os"
	"os/exec"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
)

// httpTransport defines a http.Transport with values that are more restrictive
// than for http.DefaultTransport.
//
// They define shorter TLS Handshake, and more agressive connection closing
// to prevent the connection hanging and reduce FD usage.
var httpTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 10 * time.Second,
	}).DialContext,
	MaxIdleConns:          2,
	IdleConnTimeout:       30 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 10 * time.Second,
	ResponseHeaderTimeout: 30 * time.Second,
}

// httpClient defines a http.Client that uses the specialized httpTransport
// (above). It also disables following redirects, as we don't expect this to be
// required for this RPC.
var httpClient = &http.Client{
	Transport: httpTransport,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func untar(ctx context.Context, path string, in *gitalypb.CreateRepositoryFromSnapshotRequest) error {
	req, err := http.NewRequest("GET", in.HttpUrl, nil)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Bad HTTP URL: %v", err)
	}

	if in.HttpAuth != "" {
		req.Header.Set("Authorization", in.HttpAuth)
	}

	rsp, err := httpClient.Do(req)
	if err != nil {
		return status.Errorf(codes.Internal, "HTTP request failed: %v", err)
	}
	defer rsp.Body.Close()

	if rsp.StatusCode < http.StatusOK || rsp.StatusCode >= http.StatusMultipleChoices {
		return status.Errorf(codes.Internal, "HTTP server: %v", rsp.Status)
	}

	cmd, err := command.New(ctx, exec.Command("tar", "-C", path, "-xvf", "-"), rsp.Body, nil, nil)
	if err != nil {
		return err
	}

	return cmd.Wait()
}

func (s *server) CreateRepositoryFromSnapshot(ctx context.Context, in *gitalypb.CreateRepositoryFromSnapshotRequest) (*gitalypb.CreateRepositoryFromSnapshotResponse, error) {
	realPath, err := helper.GetPath(in.Repository)
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(realPath); !os.IsNotExist(err) {
		return nil, status.Errorf(codes.InvalidArgument, "destination directory exists")
	}

	// Perform all operations against a temporary directory, only moving it to
	// the canonical location if retrieving and unpacking the snapshot is a
	// success
	tempRepo, tempPath, err := tempdir.NewAsRepository(ctx, in.Repository)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "couldn't create temporary directory: %v", err)
	}

	// The archive contains a partial git repository, missing a config file and
	// other important items. Initializing a new bare one and extracting the
	// archive on top of it ensures the created git repository has everything
	// it needs (especially, the config file and hooks directory).
	//
	// NOTE: The received archive is trusted *a lot*. Before pointing this RPC
	// at endpoints not under our control, it should undergo a lot of hardning.
	crr := &gitalypb.CreateRepositoryRequest{Repository: tempRepo}
	if _, err := s.CreateRepository(ctx, crr); err != nil {
		return nil, status.Errorf(codes.Internal, "couldn't create empty bare repository: %v", err)
	}

	if err := untar(ctx, tempPath, in); err != nil {
		return nil, err
	}

	if err := os.Rename(tempPath, realPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Promoting temporary directory failed: %v", err)
	}

	return &gitalypb.CreateRepositoryFromSnapshotResponse{}, nil
}
