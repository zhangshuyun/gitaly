package commit

import (
	"context"
	"fmt"
	"sort"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

func sendTreeEntries(stream gitalypb.CommitService_GetTreeEntriesServer, c catfile.Batch, revision, path string, recursive bool, sort gitalypb.GetTreeEntriesRequest_SortBy, p *gitalypb.PaginationParameter) error {
	ctx := stream.Context()

	entries, err := treeEntries(ctx, c, revision, path, "", recursive)
	if err != nil {
		return err
	}

	// We sort before we paginate to ensure consistent results with ListLastCommitsForTree
	entries, err = sortTrees(entries, sort)
	if err != nil {
		return err
	}

	cursor := ""
	if p != nil {
		entries, cursor, err = paginateTreeEntries(entries, p)
		if err != nil {
			return err
		}
	}

	if !recursive {
		if err := populateFlatPath(ctx, c, entries); err != nil {
			return err
		}
	}

	treeSender := &treeEntriesSender{stream: stream}

	if cursor != "" {
		treeSender.SetPaginationCursor(cursor)
	}

	sender := chunk.New(treeSender)
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
	response   *gitalypb.GetTreeEntriesResponse
	stream     gitalypb.CommitService_GetTreeEntriesServer
	cursor     string
	sentCursor bool
}

func (c *treeEntriesSender) Append(m proto.Message) {
	c.response.Entries = append(c.response.Entries, m.(*gitalypb.TreeEntry))
}

func (c *treeEntriesSender) Send() error {
	// To save bandwidth, we only send the cursor on the first response
	if !c.sentCursor {
		c.response.PaginationCursor = &gitalypb.PaginationCursor{NextCursor: c.cursor}
		c.sentCursor = true
	}

	return c.stream.Send(c.response)
}

func (c *treeEntriesSender) Reset() {
	c.response = &gitalypb.GetTreeEntriesResponse{}
}

func (c *treeEntriesSender) SetPaginationCursor(cursor string) {
	c.cursor = cursor
}

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
	return sendTreeEntries(stream, c, revision, path, in.Recursive, in.GetSort(), in.GetPaginationParams())
}

func paginateTreeEntries(entries []*gitalypb.TreeEntry, p *gitalypb.PaginationParameter) ([]*gitalypb.TreeEntry, string, error) {
	limit := int(p.GetLimit())
	start := p.GetPageToken()
	index := -1

	// No token means we should start from the top
	if start == "" {
		index = 0
	} else {
		for i, entry := range entries {
			if entry.GetOid() == start {
				index = i + 1
				break
			}
		}
	}

	if index == -1 {
		return nil, "", fmt.Errorf("could not find starting OID: %s", start)
	}

	if limit == 0 {
		return nil, "", nil
	}

	if limit < 0 || (index+limit >= len(entries)) {
		return entries[index:], "", nil
	}

	paginated := entries[index : index+limit]
	return paginated, paginated[len(paginated)-1].GetOid(), nil
}
