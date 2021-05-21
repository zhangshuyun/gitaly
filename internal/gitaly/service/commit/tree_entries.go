package commit

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func validateGetTreeEntriesRequest(in *gitalypb.GetTreeEntriesRequest) error {
	if err := git.ValidateRevision(in.Revision); err != nil {
		return err
	}

	if len(in.GetPath()) == 0 {
		return fmt.Errorf("empty Path")
	}

	return nil
}

func populateFlatPath(ctx context.Context, c catfile.Batch, entries []*gitalypb.TreeEntry) error {
	for _, entry := range entries {
		entry.FlatPath = entry.Path

		if entry.Type != gitalypb.TreeEntry_TREE {
			continue
		}

		for i := 1; i < defaultFlatTreeRecursion; i++ {
			subEntries, err := treeEntries(ctx, c, entry.CommitOid, string(entry.FlatPath), "", false)

			if err != nil {
				return err
			}

			if len(subEntries) != 1 || subEntries[0].Type != gitalypb.TreeEntry_TREE {
				break
			}

			entry.FlatPath = subEntries[0].Path
		}
	}

	return nil
}

func sendTreeEntries(stream gitalypb.CommitService_GetTreeEntriesServer, c catfile.Batch, revision, path string, recursive bool) error {
	ctx := stream.Context()

	entries, err := treeEntries(ctx, c, revision, path, "", recursive)
	if err != nil {
		return err
	}

	if !recursive {
		if err := populateFlatPath(ctx, c, entries); err != nil {
			return err
		}
	}

	sender := chunk.New(&treeEntriesSender{stream: stream})
	for _, e := range entries {
		if err := sender.Send(e); err != nil {
			return err
		}
	}

	return sender.Flush()
}

type treeEntriesSender struct {
	response *gitalypb.GetTreeEntriesResponse
	stream   gitalypb.CommitService_GetTreeEntriesServer
}

func (c *treeEntriesSender) Append(m proto.Message) {
	c.response.Entries = append(c.response.Entries, m.(*gitalypb.TreeEntry))
}

func (c *treeEntriesSender) Send() error { return c.stream.Send(c.response) }
func (c *treeEntriesSender) Reset()      { c.response = &gitalypb.GetTreeEntriesResponse{} }

func (s *server) GetTreeEntries(in *gitalypb.GetTreeEntriesRequest, stream gitalypb.CommitService_GetTreeEntriesServer) error {
	ctxlogrus.Extract(stream.Context()).WithFields(log.Fields{
		"Revision": in.Revision,
		"Path":     in.Path,
	}).Debug("GetTreeEntries")

	if err := validateGetTreeEntriesRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "TreeEntry: %v", err)
	}

	repo := s.localrepo(in.GetRepository())

	c, err := s.catfileCache.BatchProcess(stream.Context(), repo)
	if err != nil {
		return err
	}

	revision := string(in.GetRevision())
	path := string(in.GetPath())
	return sendTreeEntries(stream, c, revision, path, in.Recursive)
}
