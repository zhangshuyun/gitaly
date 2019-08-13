package commit

import (
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func sendTreeEntry(stream gitalypb.CommitService_TreeEntryServer, c *catfile.Batch, revision, path string, limit int64) error {
	treeEntry, err := NewTreeEntryFinder(c).FindByRevisionAndPath(revision, path)
	if err != nil {
		return err
	}

	if treeEntry == nil || len(treeEntry.Oid) == 0 {
		return helper.DecorateError(codes.Unavailable, stream.Send(&gitalypb.TreeEntryResponse{}))
	}

	if treeEntry.Type == gitalypb.TreeEntry_COMMIT {
		response := &gitalypb.TreeEntryResponse{
			Type: gitalypb.TreeEntryResponse_COMMIT,
			Mode: treeEntry.Mode,
			Oid:  treeEntry.Oid,
		}
		if err := stream.Send(response); err != nil {
			return status.Errorf(codes.Unavailable, "TreeEntry: send: %v", err)
		}

		return nil
	}

	if treeEntry.Type == gitalypb.TreeEntry_TREE {
		treeInfo, err := c.Info(treeEntry.Oid)
		if err != nil {
			return err
		}

		response := &gitalypb.TreeEntryResponse{
			Type: gitalypb.TreeEntryResponse_TREE,
			Oid:  treeEntry.Oid,
			Size: treeInfo.Size,
			Mode: treeEntry.Mode,
		}
		return helper.DecorateError(codes.Unavailable, stream.Send(response))
	}

	objectInfo, err := c.Info(treeEntry.Oid)
	if err != nil {
		return status.Errorf(codes.Internal, "TreeEntry: %v", err)
	}

	if strings.ToLower(treeEntry.Type.String()) != objectInfo.Type {
		return status.Errorf(
			codes.Internal,
			"TreeEntry: mismatched object type: tree-oid=%s object-oid=%s entry-type=%s object-type=%s",
			treeEntry.Oid, objectInfo.Oid, treeEntry.Type.String(), objectInfo.Type,
		)
	}

	dataLength := objectInfo.Size
	if limit > 0 && dataLength > limit {
		dataLength = limit
	}

	response := &gitalypb.TreeEntryResponse{
		Type: gitalypb.TreeEntryResponse_BLOB,
		Oid:  objectInfo.Oid,
		Size: objectInfo.Size,
		Mode: treeEntry.Mode,
	}
	if dataLength == 0 {
		return helper.DecorateError(codes.Unavailable, stream.Send(response))
	}

	blobReader, err := c.Blob(objectInfo.Oid)
	if err != nil {
		return err
	}

	sw := streamio.NewWriter(func(p []byte) error {
		response.Data = p

		if err := stream.Send(response); err != nil {
			return status.Errorf(codes.Unavailable, "TreeEntry: send: %v", err)
		}

		// Use a new response so we don't send other fields (Size, ...) over and over
		response = &gitalypb.TreeEntryResponse{}

		return nil
	})

	_, err = io.CopyN(sw, blobReader, dataLength)
	return err
}

func (s *server) TreeEntry(in *gitalypb.TreeEntryRequest, stream gitalypb.CommitService_TreeEntryServer) error {
	if err := validateRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "TreeEntry: %v", err)
	}

	requestPath := string(in.GetPath())
	// path.Dir("api/docs") => "api" Correct!
	// path.Dir("api/docs/") => "api/docs" WRONG!
	if len(requestPath) > 1 {
		requestPath = strings.TrimRight(requestPath, "/")
	}

	c, err := catfile.New(stream.Context(), in.Repository)
	if err != nil {
		return err
	}

	return sendTreeEntry(stream, c, string(in.GetRevision()), requestPath, in.GetLimit())
}

func validateRequest(in *gitalypb.TreeEntryRequest) error {
	if err := git.ValidateRevision(in.Revision); err != nil {
		return err
	}

	if len(in.GetPath()) == 0 {
		return fmt.Errorf("empty Path")
	}

	return nil
}
