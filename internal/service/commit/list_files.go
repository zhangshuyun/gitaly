package commit

import (
	"bytes"

	log "github.com/Sirupsen/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
)

func (s *server) ListFiles(in *pb.ListFilesRequest, stream pb.CommitService_ListFilesServer) error {
	grpc_logrus.Extract(stream.Context()).WithFields(log.Fields{
		"Revision": in.GetRevision(),
	}).Debug("ListFiles")

	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}

	revision := in.GetRevision()
	if len(revision) == 0 {
		revision, err = defaultBranchName(stream.Context(), repoPath)
		if err != nil {
			return grpc.Errorf(codes.NotFound, "Revision not found %q", in.GetRevision())
		}
	}
	if !helper.IsValidRef(stream.Context(), repoPath, string(revision)) {
		return stream.Send(&pb.ListFilesResponse{})
	}

	cmd, err := helper.GitCommandReader(stream.Context(), "--git-dir", repoPath, "ls-tree", "-z", "-r", "--full-tree", "--full-name", "--", string(revision))
	if err != nil {
		return grpc.Errorf(codes.Internal, err.Error())
	}
	defer cmd.CleanUpProcessGroup(stream.Context())

	scanner := lines.ScanWithDelimiter([]byte{'\x00'})

	return lines.Send(cmd, listFilesWriter(stream), scanner)
}

func listFilesWriter(stream pb.CommitService_ListFilesServer) lines.Sender {
	return func(objs [][]byte) error {
		paths := make([][]byte, 0)
		for _, obj := range objs {
			data := bytes.SplitN(obj, []byte{'\t'}, 2)
			meta := bytes.SplitN(data[0], []byte{' '}, 3)
			if bytes.Equal(meta[1], []byte("blob")) {
				paths = append(paths, data[1])
			}
		}
		return stream.Send(&pb.ListFilesResponse{Paths: paths})
	}
}
