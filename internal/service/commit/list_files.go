package commit

import (
	"fmt"
	"io"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func (s *server) ListFiles(in *gitalypb.ListFilesRequest, stream gitalypb.CommitService_ListFilesServer) error {
	grpc_logrus.Extract(stream.Context()).WithFields(log.Fields{
		"Revision": in.GetRevision(),
	}).Debug("ListFiles")

	if err := validateListFilesRequest(in); err != nil {
		return err
	}

	repo := in.Repository
	if _, err := helper.GetRepoPath(repo); err != nil {
		return err
	}

	revision := string(in.GetRevision())
	if len(revision) == 0 {
		defaultBranch, err := defaultBranchName(stream.Context(), repo)
		if err != nil {
			return helper.DecorateError(codes.NotFound, fmt.Errorf("revision not found %q", revision))
		}

		revision = string(defaultBranch)
	}

	if !git.IsValidRef(stream.Context(), repo, revision) {
		return stream.Send(&gitalypb.ListFilesResponse{})
	}

	if err := listFiles(repo, revision, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func validateListFilesRequest(in *gitalypb.ListFilesRequest) error {
	if err := git.ValidateRevisionAllowEmpty(in.Revision); err != nil {
		return helper.ErrInvalidArgument(err)
	}
	return nil
}

func listFiles(repo *gitalypb.Repository, revision string, stream gitalypb.CommitService_ListFilesServer) error {
	args := []string{"ls-tree", "-z", "-r", "--full-tree", "--full-name", "--", revision}
	cmd, err := git.Command(stream.Context(), repo, args...)
	if err != nil {
		return err
	}

	sender := chunk.New(&listFilesSender{stream: stream})

	for parser := lstree.NewParser(cmd); ; {
		entry, err := parser.NextEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if entry.Type != lstree.Blob {
			continue
		}

		if err := sender.Send([]byte(entry.Path)); err != nil {
			return err
		}
	}

	return sender.Flush()
}

type listFilesSender struct {
	stream   gitalypb.CommitService_ListFilesServer
	response *gitalypb.ListFilesResponse
}

func (s *listFilesSender) Reset()      { s.response = &gitalypb.ListFilesResponse{} }
func (s *listFilesSender) Send() error { return s.stream.Send(s.response) }
func (s *listFilesSender) Append(it chunk.Item) {
	s.response.Paths = append(s.response.Paths, it.([]byte))
}
