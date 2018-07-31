package smarthttp

import (
	"fmt"
	"io"
	"os/exec"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/streamio"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	deepenCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_smarthttp_deepen_count",
			Help: "Number of git-upload-pack requests processed that contained a 'deepen' message",
		},
	)
)

func init() {
	prometheus.MustRegister(deepenCount)
}

func (s *server) PostUploadPack(stream pb.SmartHTTPService_PostUploadPackServer) error {
	req, err := stream.Recv() // First request contains Repository only
	if err != nil {
		return err
	}

	grpc_logrus.Extract(stream.Context()).WithFields(log.Fields{
		"GitProtocol": req.GitProtocol,
	}).Debug("PostUploadPack")

	if err := validateUploadPackRequest(req); err != nil {
		return err
	}

	stdinReader := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	pr, pw := io.Pipe()
	defer pw.Close()
	stdin := io.TeeReader(stdinReader, pw)
	deepenCh := make(chan bool, 1)
	go func() {
		deepenCh <- scanDeepen(pr)
	}()

	stdout := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&pb.PostUploadPackResponse{Data: p})
	})

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

	args = append(args, "upload-pack", "--stateless-rpc", repoPath)

	osCommand := exec.Command(command.GitPath(), args...)
	cmd, err := command.New(stream.Context(), osCommand, stdin, stdout, nil, env...)

	if err != nil {
		return status.Errorf(codes.Unavailable, "PostUploadPack: cmd: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		pw.Close() // ensure scanDeepen returns
		if _, ok := command.ExitStatus(err); ok && <-deepenCh {
			// We have seen a 'deepen' message in the request. It is expected that
			// git-upload-pack has a non-zero exit status: don't treat this as an
			// error.
			deepenCount.Inc()
			return nil
		}
		return status.Errorf(codes.Unavailable, "PostUploadPack: %v", err)
	}

	return nil
}

func validateUploadPackRequest(req *pb.PostUploadPackRequest) error {
	if req.Data != nil {
		return status.Errorf(codes.InvalidArgument, "PostUploadPack: non-empty Data")
	}

	return nil
}
