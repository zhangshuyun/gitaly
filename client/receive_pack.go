package client

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/stream"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc"
)

// ReceivePack proxies an SSH git-receive-pack (git push) session to Gitaly
func ReceivePack(ctx context.Context, conn *grpc.ClientConn, stdin io.Reader, stdout, stderr io.Writer, req *gitalypb.SSHReceivePackRequest) (int32, error) {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	ssh := gitalypb.NewSSHServiceClient(conn)
	receivePackStream, err := ssh.SSHReceivePack(ctx2)
	if err != nil {
		return 0, err
	}

	if err = receivePackStream.Send(req); err != nil {
		return 0, err
	}

	inWriter := streamio.NewWriter(func(p []byte) error {
		return receivePackStream.Send(&gitalypb.SSHReceivePackRequest{Stdin: p})
	})

	return stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return receivePackStream.Recv()
	}, func(errC chan error) {
		_, errRecv := io.Copy(inWriter, stdin)
		receivePackStream.CloseSend()
		errC <- errRecv
	}, stdout, stderr)
}
