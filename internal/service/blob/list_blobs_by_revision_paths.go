package blob

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"strings"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"golang.org/x/net/context"
)

// BatchBlobMaxReadLimit sets the limit for the data in messages when performing
// a batch request
const BatchBlobMaxReadLimit = 1 << 20

// For batch blobs fetching by revision path, for example, for commit
// abcdef1234 give me CHANGELOG. Given its a batch endpoint we limit the
// response size to BatchBlobMaxReadLimit. Errors are logged, but for clients
// not visible, unless the repository can't be found. If #input != #output that
// means that either the item was not found or an internal error occured. It is
// the responsibilty of the client to gracefully handle these cases.
func (s *server) ListBlobsByRevisionPath(in *pb.ListBlobsByRevisionPathRequest, stream pb.BlobService_ListBlobsByRevisionPathServer) error {
	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}

	for _, revisionPath := range in.RevisionPaths {
		revision := revisionPath.GetCommitOid()
		path := revisionPath.GetPath()
		limit := readLimit(revisionPath.Limit)
		if revision == "" || path == "" {
			continue
		}

		blobOid, err := lookupRevision(stream.Context(), in.Repository, fmt.Sprintf("%s:%s", revision, path))
		if err != nil {
			grpc_logrus.Extract(stream.Context()).WithError(err)
			continue
		}

		if err := sendBlobMsg(stream, repoPath, blobOid, limit); err != nil {
			grpc_logrus.Extract(stream.Context()).WithError(err)
		}
	}

	return nil
}

func sendBlobMsg(stream pb.BlobService_ListBlobsByRevisionPathServer, repoPath, oid string, limit int64) error {
	stdinReader, stdinWriter := io.Pipe()

	cmdArgs := []string{"--git-dir", repoPath, "cat-file", "--batch"}
	cmd, err := command.New(stream.Context(), exec.Command(command.GitPath(), cmdArgs...), stdinReader, nil, nil)
	if err != nil {
		return fmt.Errorf("ListBlobsByRevisionPath: cmd: %v", err)
	}
	defer stdinWriter.Close()
	defer stdinReader.Close()

	if _, err := fmt.Fprintln(stdinWriter, oid); err != nil {
		return fmt.Errorf("ListBlobsByRevisionPath: stdin write: %v", err)
	}
	stdinWriter.Close()

	stdout := bufio.NewReader(cmd)

	objectInfo, err := catfile.ParseObjectInfo(stdout)
	if err != nil {
		return fmt.Errorf("ListCommitsByOid: %v", err)
	}
	if objectInfo.Type != "blob" {
		return fmt.Errorf("ListCommitsByOid: expected type blob, found: %s", objectInfo.Type)
	}

	sw := streamio.NewWriter(func(p []byte) error {
		msg := &pb.ListBlobsByRevisionPathResponse{
			Oid:  objectInfo.Oid,
			Size: objectInfo.Size,
			Data: p,
		}
		return stream.Send(msg)
	})

	n, err := io.Copy(sw, io.LimitReader(stdout, limit))
	if err != nil {
		return fmt.Errorf("ListBlobsByRevisionPath: send: %v", err)
	}
	if n != limit {
		return fmt.Errorf("ListBlobsByRevisionPath: short send: %d/%d bytes", n, objectInfo.Size)
	}

	return nil
}

// Copied from `internal/service/commit/languages.go`
// TODO consider making a plumbing lib and put it there
func lookupRevision(ctx context.Context, repo *pb.Repository, revision string) (string, error) {
	revParse, err := git.Command(ctx, repo, "rev-parse", revision)
	if err != nil {
		return "", err
	}

	revParseBytes, err := ioutil.ReadAll(revParse)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(revParseBytes)), nil
}

func readLimit(initialLimit int64) int64 {
	if initialLimit <= 0 {
		return BatchBlobMaxReadLimit
	}

	if initialLimit < BatchBlobMaxReadLimit {
		return initialLimit
	}

	return BatchBlobMaxReadLimit
}
