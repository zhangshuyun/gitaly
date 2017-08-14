package blob

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/streamio"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) GetBlob(in *pb.GetBlobRequest, stream pb.BlobService_GetBlobServer) error {
	if err := validateRequest(in); err != nil {
		return grpc.Errorf(codes.InvalidArgument, "GetBlob: %v", err)
	}

	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}

	stdinReader, stdinWriter := io.Pipe()

	cmdArgs := []string{"--git-dir", repoPath, "cat-file", "--batch"}
	cmd, err := helper.NewCommand(stream.Context(), exec.Command(helper.GitPath(), cmdArgs...), stdinReader, nil, nil)
	if err != nil {
		return grpc.Errorf(codes.Internal, "GetBlob: cmd: %v", err)
	}
	defer cmd.Kill(stream.Context())
	defer stdinWriter.Close()
	defer stdinReader.Close()

	if _, err := fmt.Fprintln(stdinWriter, in.Oid); err != nil {
		return grpc.Errorf(codes.Internal, "GetBlob: stdin write: %v", err)
	}
	stdinWriter.Close()

	stdout := bufio.NewReader(cmd)

	objectInfo, err := catfile.ParseObjectInfo(stdout)
	if err != nil {
		return grpc.Errorf(codes.Internal, "GetBlob: %v", err)
	}
	if objectInfo.Type != "blob" {
		return helper.DecorateError(codes.Unavailable, stream.Send(&pb.GetBlobResponse{}))
	}

	readLimit := objectInfo.Size
	if in.Limit >= 0 && in.Limit < readLimit {
		readLimit = in.Limit
	}
	firstMessage := &pb.GetBlobResponse{
		Size: objectInfo.Size,
		Oid:  objectInfo.Oid,
	}

	if readLimit == 0 {
		return helper.DecorateError(codes.Unavailable, stream.Send(firstMessage))
	}

	sw := streamio.NewWriter(func(p []byte) error {
		msg := &pb.GetBlobResponse{}
		if firstMessage != nil {
			msg = firstMessage
			firstMessage = nil
		}
		msg.Data = p
		return stream.Send(msg)
	})

	n, err := io.Copy(sw, io.LimitReader(stdout, readLimit))
	if err != nil {
		return grpc.Errorf(codes.Unavailable, "GetBlob: send: %v", err)
	}
	if n != readLimit {
		return grpc.Errorf(codes.Unavailable, "GetBlob: short send: %d/%d bytes", n, objectInfo.Size)
	}

	return nil
}

func validateRequest(in *pb.GetBlobRequest) error {
	if len(in.GetOid()) == 0 {
		return fmt.Errorf("empty Oid")
	}
	return nil
}
