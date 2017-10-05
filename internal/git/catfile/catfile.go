package catfile

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/command"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// ObjectInfo represents a header returned by `git cat-file --batch`
type ObjectInfo struct {
	Oid  string
	Type string
	Size int64
}

// Handler takes care of writing to stdin and reading from stdout of
// `git cat-file --batch`
type Handler func(io.Writer, *bufio.Reader) error

// CatFile fetches the tree entries information using git cat-file. It
// calls the handler with the TreeEntry slice, and an stdin reader and a stdout
// writer in case the handler wants to perform addition cat-file operations.
func CatFile(ctx context.Context, repoPath string, handler Handler) error {
	stdinReader, stdinWriter := io.Pipe()
	cmdArgs := []string{"--git-dir", repoPath, "cat-file", "--batch"}
	cmd, err := command.New(ctx, exec.Command(command.GitPath(), cmdArgs...), stdinReader, nil, nil)
	if err != nil {
		return grpc.Errorf(codes.Internal, "CatFile: cmd: %v", err)
	}
	defer stdinWriter.Close()
	defer stdinReader.Close()

	stdout := bufio.NewReader(cmd)

	return handler(stdinWriter, stdout)
}

// ParseObjectInfo reads and parses one header line from `git cat-file --batch`
func ParseObjectInfo(stdout *bufio.Reader) (*ObjectInfo, error) {
	infoLine, err := stdout.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read info line: %v", err)
	}

	infoLine = strings.TrimSuffix(infoLine, "\n")
	if strings.HasSuffix(infoLine, " missing") {
		return &ObjectInfo{}, nil
	}

	info := strings.Split(infoLine, " ")
	if len(info) != 3 {
		return nil, fmt.Errorf("strings split: expected %d strings, got %v", len(info), info)
	}

	objectSizeStr := info[2]
	objectSize, err := strconv.ParseInt(objectSizeStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse object size: %v", err)
	}

	return &ObjectInfo{
		Oid:  info[0],
		Type: info[1],
		Size: objectSize,
	}, nil
}
