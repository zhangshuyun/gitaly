package smarthttp

import (
	"io"
	"os/exec"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/streamio"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	grpc_logrus.Extract(stream.Context()).Debug("PostUploadPack")

	req, err := stream.Recv() // First request contains Repository only
	if err != nil {
		return err
	}
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
	repoPath, err := helper.GetRepoPath(req.Repository)
	if err != nil {
		return err
	}

	osCommand := exec.Command(helper.GitPath(), "upload-pack", "--stateless-rpc", repoPath)
	cmd, err := helper.NewCommand(stream.Context(), osCommand, stdin, stdout, nil)

	if err != nil {
		return grpc.Errorf(codes.Unavailable, "PostUploadPack: cmd: %v", err)
	}
	defer cmd.Kill(stream.Context())

	if err := cmd.Wait(); err != nil {
		pw.Close() // ensure scanDeepen returns
		if _, ok := helper.ExitStatus(err); ok && <-deepenCh {
			// We have seen a 'deepen' message in the request. It is expected that
			// git-upload-pack has a non-zero exit status: don't treat this as an
			// error.
			deepenCount.Inc()
			return nil
		}
		return grpc.Errorf(codes.Unavailable, "PostUploadPack: cmd wait for %v: %v", cmd.Args, err)
	}

	return nil
}

func validateUploadPackRequest(req *pb.PostUploadPackRequest) error {
	if req.Data != nil {
		return grpc.Errorf(codes.InvalidArgument, "PostUploadPack: non-empty Data")
	}

	return nil
}
