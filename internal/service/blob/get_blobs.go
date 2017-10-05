package blob

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) GetBlobs(in *pb.GetBlobsRequest, stream pb.BlobService_GetBlobsServer) error {
	if err := validateRequests(in); err != nil {
		return grpc.Errorf(codes.InvalidArgument, "GetBlob: %v", err)
	}

	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}

	return getBlobs(stream.Context(), repoPath, in.Oids, in.Limit, func(size int64, oid string, data []byte) error {
		resp := &pb.GetBlobsResponse{
			Size: size,
			Oid:  oid,
			Data: data,
		}

		return stream.Send(resp)
	})
}

func getBlobs(ctx context.Context, repoPath string, oids []string, limit int64, sender blobSender) error {
	stdinReader, stdinWriter := io.Pipe()

	cmdArgs := []string{"--git-dir", repoPath, "cat-file", "--batch"}
	cmd, err := command.New(ctx, exec.Command(command.GitPath(), cmdArgs...), stdinReader, nil, nil)
	if err != nil {
		return grpc.Errorf(codes.Internal, "getBlob: cmd: %v", err)
	}
	defer stdinWriter.Close()
	defer stdinReader.Close()

	stdout := bufio.NewReader(cmd)

	var (
		firstMessage bool
		objectInfo   *catfile.ObjectInfo
	)

	sw := streamio.NewWriter(func(p []byte) error {
		if firstMessage {
			firstMessage = false
			return sender(objectInfo.Size, objectInfo.Oid, p)
		}
		return sender(0, "", p)
	})

	for _, oid := range oids {
		firstMessage = true
		if _, err = fmt.Fprintln(stdinWriter, oid); err != nil {
			return grpc.Errorf(codes.Internal, "getBlob: stdin write: %v", err)
		}

		objectInfo, err = catfile.ParseObjectInfo(stdout)
		if err != nil {
			return grpc.Errorf(codes.Internal, "getBlob: %v", err)
		}
		if objectInfo.Type != "blob" {
			return helper.DecorateError(codes.Unavailable, sender(0, "", nil))
		}

		readLimit := objectInfo.Size
		if limit >= 0 && limit < readLimit {
			readLimit = limit
		}

		if readLimit == 0 {
			err := sender(objectInfo.Size, objectInfo.Oid, nil)
			if err != nil {
				return grpc.Errorf(codes.Unavailable, "getBlob: send: %v", err)
			}
		}

		n, err := io.Copy(sw, io.LimitReader(stdout, readLimit))
		if err != nil {
			return grpc.Errorf(codes.Unavailable, "getBlob: send: %v", err)
		}
		if n != readLimit {
			return grpc.Errorf(codes.Unavailable, "getBlob: short send: %d/%d bytes", n, objectInfo.Size)
		}

		// +1 because of newlines...
		if rest := objectInfo.Size - readLimit + 1; rest > 0 {
			n, err := io.Copy(ioutil.Discard, io.LimitReader(stdout, rest))
			if err != nil {
				return grpc.Errorf(codes.Unavailable, "getBlob: read: %v", err)
			}
			if n != rest {
				return grpc.Errorf(codes.Unavailable, "getBlob: short send: %d/%d bytes", n, rest)
			}
		}
	}
	stdinWriter.Close()

	return cmd.Wait()
}

func validateRequests(in *pb.GetBlobsRequest) error {
	if len(in.GetOids()) == 0 {
		return fmt.Errorf("no Oids specified")
	}
	for _, oid := range in.GetOids() {
		if len(oid) == 0 {
			return fmt.Errorf("empty Oid found")
		}
	}
	return nil
}
