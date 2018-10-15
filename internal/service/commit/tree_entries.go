package commit

import (
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var maxTreeEntries = 1000

func validateGetTreeEntriesRequest(in *gitalypb.GetTreeEntriesRequest) error {
	if len(in.GetRevision()) == 0 {
		return fmt.Errorf("empty Revision")
	}

	if len(in.GetPath()) == 0 {
		return fmt.Errorf("empty Path")
	}

	return nil
}

func populateFlatPath(c *catfile.Batch, entries []*gitalypb.TreeEntry) error {
	for _, entry := range entries {
		entry.FlatPath = entry.Path

		if entry.Type != gitalypb.TreeEntry_TREE {
			continue
		}

		for {
			subentries, err := treeEntries(c, entry.CommitOid, string(entry.FlatPath), "", false)

			if err != nil {
				return err
			}
			if len(subentries) != 1 || subentries[0].Type != gitalypb.TreeEntry_TREE {
				break
			}

			entry.FlatPath = subentries[0].Path
		}
	}

	return nil
}

func sendTreeEntries(stream gitalypb.CommitService_GetTreeEntriesServer, c *catfile.Batch, revision, path string, recursive bool) error {
	entries, err := treeEntries(c, revision, path, "", recursive)
	if err != nil {
		return err
	}

	if !recursive {
		if err := populateFlatPath(c, entries); err != nil {
			return err
		}
	}

	for len(entries) > maxTreeEntries {
		chunk := &gitalypb.GetTreeEntriesResponse{
			Entries: entries[:maxTreeEntries],
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
		entries = entries[maxTreeEntries:]
	}

	if len(entries) > 0 {
		return stream.Send(&gitalypb.GetTreeEntriesResponse{Entries: entries})
	}

	return nil
}

func (s *server) GetTreeEntries(in *gitalypb.GetTreeEntriesRequest, stream gitalypb.CommitService_GetTreeEntriesServer) error {
	grpc_logrus.Extract(stream.Context()).WithFields(log.Fields{
		"Revision": in.Revision,
		"Path":     in.Path,
	}).Debug("GetTreeEntries")

	if err := validateGetTreeEntriesRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "TreeEntry: %v", err)
	}

	c, err := catfile.New(stream.Context(), in.Repository)
	if err != nil {
		return err
	}

	revision := string(in.GetRevision())
	path := string(in.GetPath())
	return sendTreeEntries(stream, c, revision, path, in.Recursive)
}
