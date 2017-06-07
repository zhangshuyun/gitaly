package blob

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func (s *server) BlobFromCommit(in *pb.BlobFromCommitRequest, stream pb.Blob_BlobFromCommitServer) error {
	if err := validateRequest(in); err != nil {
		return grpc.Errorf(codes.InvalidArgument, "BlobFromCommit: %v", err)
	}

	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}

	stdinReader, stdinWriter := io.Pipe()

	cmdArgs := []string{
		"--git-dir", repoPath,
		"cat-file",
		"--batch",
	}
	cmd, err := helper.NewCommand(exec.Command("git", cmdArgs...), stdinReader, nil, nil)
	if err != nil {
		return grpc.Errorf(codes.Internal, "BlobFromCommit: cmd: %v", err)
	}
	defer cmd.Kill()
	// Needed?
	defer stdinWriter.Close()
	defer stdinReader.Close()

	dirName := path.Dir(string(in.GetPath()))
	if dirName == "." {
		dirName = ""
	}
	baseName := path.Base(string(in.GetPath()))

	treeObject := fmt.Sprintf("%s^{tree}:%s\n", in.GetCommitId(), dirName) // TODO: Escape both args
	stdinWriter.Write([]byte(treeObject))

	buf := bufio.NewReader(cmd)
	line, err := buf.ReadBytes('\n')
	if err != nil {
		return grpc.Errorf(codes.Internal, "BlobFromCommit: stdout initial read: %v", err)
	}

	if bytes.HasSuffix(line, []byte("missing\n")) {
		response := &pb.BlobFromCommitResponse{
			Found: false,
		}
		stream.Send(response)

		return nil
	}

	treeInfo := bytes.Split(line, []byte(" "))
	treeSizeStr := string(treeInfo[2])
	treeSizeStr = strings.TrimSpace(treeSizeStr)
	treeSize, err := strconv.ParseInt(treeSizeStr, 10, 0)
	if err != nil {
		return grpc.Errorf(codes.Internal, "BlobFromCommit: parse tree size: %v", err)
	}

	var mode, path []byte
	sha := make([]byte, 20)
	entryFound := false
	bytesLeft := int(treeSize) // XXX: should it be int?

	for {
		mode, err = buf.ReadBytes(' ')
		if err != nil {
			return grpc.Errorf(codes.Internal, "BlobFromCommit: read entry mode: %v", err)
		}
		bytesLeft -= len(mode)

		path, err = buf.ReadBytes('\x00')
		if err != nil {
			return grpc.Errorf(codes.Internal, "BlobFromCommit: read entry path: %v", err)
		}
		bytesLeft -= len(path)

		if n, _ := buf.Read(sha); n != 20 {
			return grpc.Errorf(codes.Internal, "BlobFromCommit: read entry sha: %v", err)
		}
		bytesLeft -= len(sha)

		if strings.TrimSuffix(string(path), "\x00") == baseName { // XXX
			entryFound = true
			break
		}
	}

	if !entryFound {
		response := &pb.BlobFromCommitResponse{
			Found: false,
		}
		stream.Send(response)

		return nil
	}

	sha = []byte(fmt.Sprintf("%02x", sha))

	if string(mode) == "160000 " { // XXX: Trim that
		response := &pb.BlobFromCommitResponse{
			Id:    string(sha),
			Found: true,
		}
		stream.Send(response)

		return nil
	}

	log.Printf("mode=%s path=%s", mode, path)

	buf.Discard(bytesLeft + 1) // There's a linefeed at the end
	stdinWriter.Write([]byte(fmt.Sprintf("%s\n", sha)))
	stdinWriter.Close()

	line, err = buf.ReadBytes('\n')
	if err != nil {
		return grpc.Errorf(codes.Internal, "BlobFromCommit: stdout initial read: %v", err)
	}
	blob := make([]byte, s.MsgSizeThreshold)
	blobInfo := bytes.Split(line, []byte(" "))

	blobSizeStr := string(blobInfo[2])
	blobSizeStr = strings.TrimSpace(blobSizeStr)
	blobSize, err := strconv.ParseInt(blobSizeStr, 10, 0)
	if err != nil {
		return grpc.Errorf(codes.Internal, "BlobFromCommit: parse blob size: %v", err)
	}

	for {
		n, err := buf.Read(blob)
		if err != nil && err != io.EOF {
			return grpc.Errorf(codes.Internal, "BlobFromCommit: stdout read: %v", err)
		}
		if n == 0 {
			break
		}

		response := &pb.BlobFromCommitResponse{
			Id:    string(blobInfo[0]),
			Blob:  blob[:n],
			Size:  int32(blobSize),
			Found: true,
		}
		stream.Send(response)
	}

	return nil
}

func validateRequest(in *pb.BlobFromCommitRequest) error {
	if in.GetCommitId() == "" {
		return fmt.Errorf("empty CommitId")
	}

	if len(in.GetPath()) == 0 {
		return fmt.Errorf("empty Path")
	}

	return nil
}
