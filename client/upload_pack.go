package client

import (
	"io"

	"golang.org/x/net/context"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	pbhelper "gitlab.com/gitlab-org/gitaly-proto/go/helper"
)

// UploadPack sends a git-pack payload to Gitaly
func (cli *Client) UploadPack(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, req *pb.SSHUploadPackRequest) (int32, error) {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	ssh := pb.NewSSHClient(cli.conn)
	stream, err := ssh.SSHUploadPack(ctx2)
	if err != nil {
		return 0, err
	}

	if err = stream.Send(req); err != nil {
		return 0, err
	}

	inWriter := pbhelper.NewSendWriter(func(p []byte) error {
		return stream.Send(&pb.SSHUploadPackRequest{Stdin: p})
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

	if err := <-errC; err != nil {
		return exitStatus, err
	}
	return exitStatus, errRecv
}
