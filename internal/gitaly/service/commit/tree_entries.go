package commit

import (
	"context"
	"fmt"
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
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

func sendTreeEntries(stream gitalypb.CommitService_GetTreeEntriesServer, c catfile.Batch, revision, path string, recursive bool, sort gitalypb.GetTreeEntriesRequest_SortBy) error {
	ctx := stream.Context()

	entries, err := treeEntries(ctx, c, revision, path, "", recursive)
	if err != nil {
		return err
	}

	entries, err = sortTrees(entries, sort)
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

func sortTrees(entries []*gitalypb.TreeEntry, sortBy gitalypb.GetTreeEntriesRequest_SortBy) ([]*gitalypb.TreeEntry, error) {
	if sortBy == gitalypb.GetTreeEntriesRequest_DEFAULT {
		return entries, nil
	}

	var err error

	sort.SliceStable(entries, func(i, j int) bool {
		a, firstError := toLsTreeEnum(entries[i].Type)
		b, secondError := toLsTreeEnum(entries[j].Type)

		if firstError != nil {
			err = firstError
		} else if secondError != nil {
			err = secondError
		}

		return a < b
	})

	return entries, err
}

// This is used to match the sorting order given by getLSTreeEntries
func toLsTreeEnum(input gitalypb.TreeEntry_EntryType) (lstree.ObjectType, error) {
	switch input {
	case gitalypb.TreeEntry_TREE:
		return lstree.Tree, nil
	case gitalypb.TreeEntry_COMMIT:
		return lstree.Submodule, nil
	case gitalypb.TreeEntry_BLOB:
		return lstree.Blob, nil
	default:
		return -1, lstree.ErrParse
	}
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
	sort := in.GetSort()
	return sendTreeEntries(stream, c, revision, path, in.Recursive, sort)
}
