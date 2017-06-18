package client

import (
	"io"

	"google.golang.org/grpc"

	"golang.org/x/net/context"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	pbhelper "gitlab.com/gitlab-org/gitaly-proto/go/helper"
)

// ReceivePack gets git-pack from Gitaly
func ReceivePack(conn *grpc.ClientConn, ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, req *pb.SSHReceivePackRequest) (int32, error) {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	ssh := pb.NewSSHClient(conn)
	stream, err := ssh.SSHReceivePack(ctx2)
	if err != nil {
		return 0, err
	}

	if err = stream.Send(req); err != nil {
		return 0, err
	}

	inWriter := pbhelper.NewSendWriter(func(p []byte) error {
		return stream.Send(&pb.SSHReceivePackRequest{Stdin: p})
	})

	errC := make(chan error, 1)

	go func() {
		_, errRecv := io.Copy(inWriter, stdin)
		stream.CloseSend()
		errC <- errRecv
	}()

	exitStatus, errRecv := recvStdoutStderrStream(func() (stdoutStderrResponse, error) {
		return stream.Recv()
	}, stdout, stderr)

	if err := <-errC; err != nil && err != io.EOF {
		return exitStatus, err
	}

	return exitStatus, errRecv
}
