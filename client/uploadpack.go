package client

import (
	"io"
	"io/ioutil"
	"time"

	"golang.org/x/net/context"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	pbhelper "gitlab.com/gitlab-org/gitaly-proto/go/helper"
)

// UploadPack sends a git-pack payload to Gitaly
func (cli *Client) UploadPack(stdin io.Reader, stdout, stderr io.Writer, repo string) (int32, error) {
	ssh := pb.NewSSHClient(cli.conn)
	// TODO: Set a proper timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//ctx := context.Background()
	stream, err := ssh.SSHUploadPack(ctx)
	if err != nil {
		return 1, err
	}

	req := &pb.SSHUploadPackRequest{
		Repository: &pb.Repository{Path: repo},
	}

	if err = stream.Send(req); err != nil {
		return 1, err
	}

	inWriter := pbhelper.NewSendWriter(func(p []byte) error {
		return stream.Send(&pb.SSHUploadPackRequest{Stdin: p})
	})

	errC := make(chan error, 1)

	go func() {
		buf := make([]byte, 1024)
		for {
			n, errRead := stdin.Read(buf)
			if errRead == io.EOF {
				// NOTE: nasty hack to close stdin
				inWriter.Write(nil)
				errC <- nil
			}
			if errRead != nil {
				errC <- errRead
			}
			if n > 0 {
				_, errWrite := inWriter.Write(buf[:n])
				if errWrite != nil {
					errC <- errWrite
				}
			}
		}
	}()

	exitStatus := int32(-1)
	recv := pbhelper.NewReceiveReader(func() ([]byte, error) {
		resp, errRecv := stream.Recv()
		if errRecv != nil {
			return nil, errRecv
		}
		if resp.ExitStatus != nil {
			exitStatus = resp.GetExitStatus().GetValue()
			return nil, io.EOF
		}

		if len(resp.Stderr) > 0 {
			if _, errWrite := stderr.Write(resp.Stderr); errWrite != nil {
				return nil, errWrite
			}
			return nil, io.EOF
		}

		if len(resp.Stdout) > 0 {
			if _, errWrite := stdout.Write(resp.Stdout); errWrite != nil {
				return nil, errWrite
			}
		}

		return nil, nil
	})

	if _, err = io.Copy(ioutil.Discard, recv); err != nil {
		return 1, err
	}

	if err = stream.CloseSend(); err != nil {
		return 1, err
	}

	if _, err = io.Copy(ioutil.Discard, recv); err != nil {
		return 1, err
	}

	if err := <-errC; err != nil && err != io.EOF {
		return 1, err
	}
	return exitStatus, nil
}
