package client

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/stream"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc"
)

// UploadPack proxies an SSH git-upload-pack (git fetch) session to Gitaly
func UploadPack(ctx context.Context, conn *grpc.ClientConn, stdin io.Reader, stdout, stderr io.Writer, req *gitalypb.SSHUploadPackRequest) (int32, error) {
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()

	ssh := gitalypb.NewSSHServiceClient(conn)
	uploadPackStream, err := ssh.SSHUploadPack(ctx2)
	if err != nil {
		return 0, err
	}

	if err = uploadPackStream.Send(req); err != nil {
		return 0, err
	}

	inWriter := streamio.NewWriter(func(p []byte) error {
		return uploadPackStream.Send(&gitalypb.SSHUploadPackRequest{Stdin: p})
	})

	return stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return uploadPackStream.Recv()
	}, func(errC chan error) {
		_, errRecv := io.Copy(inWriter, stdin)
		if err := uploadPackStream.CloseSend(); err != nil && errRecv == nil {
			errC <- err
		} else {
			errC <- errRecv
		}
	}, stdout, stderr)
}

// UploadPackWithSidechannel proxies an SSH git-upload-pack (git fetch)
// session to Gitaly using a sidechannel for the raw data transfer.
func UploadPackWithSidechannel(ctx context.Context, conn *grpc.ClientConn, reg *SidechannelRegistry, stdin io.Reader, stdout, stderr io.Writer, req *gitalypb.SSHUploadPackWithSidechannelRequest) (int32, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ctx, wt := reg.Register(ctx, func(c SidechannelConn) error {
		return stream.ProxyPktLine(c, stdin, stdout, stderr)
	})
	defer wt.Close()

	sshClient := gitalypb.NewSSHServiceClient(conn)
	if _, err := sshClient.SSHUploadPackWithSidechannel(ctx, req); err != nil {
		return 0, err
	}

	if err := wt.Close(); err != nil {
		return 0, err
	}

	return 0, nil
}
