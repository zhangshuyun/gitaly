package client

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/stream"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc"
)

// UploadArchive proxies an SSH git-upload-archive (git archive --remote) session to Gitaly
func UploadArchive(ctx context.Context, conn *grpc.ClientConn, stdin io.Reader, stdout, stderr io.Writer, req *gitalypb.SSHUploadArchiveRequest) (int32, error) {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	ssh := gitalypb.NewSSHServiceClient(conn)
	uploadPackStream, err := ssh.SSHUploadArchive(ctx2)
	if err != nil {
		return 0, err
	}

	if err = uploadPackStream.Send(req); err != nil {
		return 0, err
	}

	inWriter := streamio.NewWriter(func(p []byte) error {
		return uploadPackStream.Send(&gitalypb.SSHUploadArchiveRequest{Stdin: p})
	})

	return stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return uploadPackStream.Recv()
	}, func(errC chan error) {
		_, errRecv := io.Copy(inWriter, stdin)
		uploadPackStream.CloseSend()
		errC <- errRecv
	}, stdout, stderr)
}
