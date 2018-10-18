package commit

import (
	"bytes"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
)

func (s *server) ListFiles(in *gitalypb.ListFilesRequest, stream gitalypb.CommitService_ListFilesServer) error {
	grpc_logrus.Extract(stream.Context()).WithFields(log.Fields{
		"Revision": in.GetRevision(),
	}).Debug("ListFiles")

	repo := in.Repository
	if _, err := helper.GetRepoPath(repo); err != nil {
		return err
	}

	revision := in.GetRevision()
	if len(revision) == 0 {
		var err error

		revision, err = defaultBranchName(stream.Context(), repo)
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return err
			}
			return status.Errorf(codes.NotFound, "Revision not found %q", in.GetRevision())
		}
	}
	if !git.IsValidRef(stream.Context(), repo, string(revision)) {
		return stream.Send(&gitalypb.ListFilesResponse{})
	}

	cmd, err := git.Command(stream.Context(), repo, "ls-tree", "-z", "-r", "--full-tree", "--full-name", "--", string(revision))
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, err.Error())
	}

	return lines.Send(cmd, listFilesWriter(stream), []byte{'\x00'})
}

func listFilesWriter(stream gitalypb.CommitService_ListFilesServer) lines.Sender {
	return func(objs [][]byte) error {
		paths := make([][]byte, 0)
		for _, obj := range objs {
			data := bytes.SplitN(obj, []byte{'\t'}, 2)
			if len(data) != 2 {
				return status.Errorf(codes.Internal, "ListFiles: failed parsing line")
			}

			meta := bytes.SplitN(data[0], []byte{' '}, 3)
			if len(meta) != 3 {
				return status.Errorf(codes.Internal, "ListFiles: failed parsing meta")
			}

			if bytes.Equal(meta[1], []byte("blob")) {
				paths = append(paths, data[1])
			}
		}
		return stream.Send(&gitalypb.ListFilesResponse{Paths: paths})
	}
}
