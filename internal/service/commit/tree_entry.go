package commit

import (
	"errors"
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

func findAndSendTreeEntry(stream gitalypb.CommitService_TreeEntryServer, c *catfile.Batch, revision, path string, limit int64) error {
	treeEntry, err := NewTreeEntryFinder(c).FindByRevisionAndPath(revision, path)
	if err != nil {
		return err
	}

	response, blobReader, dataLength, err := getTreeEntry(c, treeEntry, limit)
	if err != nil {
		return err
	}

	if blobReader == nil {
		return helper.DecorateError(codes.Unavailable, stream.Send(response))
	}

	sw := findTreeEntryStreamWriter(stream, response, blobReader)

	if _, err = io.CopyN(sw, blobReader, dataLength); err != nil {
		return err
	}

	return nil
}

func findAndSendTreeEntries(stream gitalypb.CommitService_FindTreeEntriesServer, c *catfile.Batch, revision string, paths [][]byte, limit int64) error {
	for _, path := range paths {
		treeEntry, err := NewTreeEntryFinder(c).FindByRevisionAndPath(revision, string(path))
		if err != nil {
			return err
		}

		response, blobReader, dataLength, err := getTreeEntry(c, treeEntry, limit)
		if err != nil {
			return err
		}

		if blobReader == nil {
			helper.DecorateError(codes.Unavailable, stream.Send(response))
			continue
		}

		sw := findTreeEntriesStreamWriter(stream, response, blobReader)

		if _, err = io.CopyN(sw, blobReader, dataLength); err != nil {
			return err
		}
	}
	return nil
}

func getTreeEntry(c *catfile.Batch, treeEntry *gitalypb.TreeEntry, limit int64) (*gitalypb.TreeEntryResponse, io.Reader, int64, error) {
	if treeEntry == nil || len(treeEntry.Oid) == 0 {
		return nil, nil, 0, helper.DecorateError(codes.Unavailable, errors.New("tree entry not found"))
	}

	if treeEntry.Type == gitalypb.TreeEntry_COMMIT {
		response := &gitalypb.TreeEntryResponse{
			Type: gitalypb.TreeEntryResponse_COMMIT,
			Mode: treeEntry.Mode,
			Oid:  treeEntry.Oid,
		}

		return response, nil, 0, nil
	}

	if treeEntry.Type == gitalypb.TreeEntry_TREE {
		treeInfo, err := c.Info(treeEntry.Oid)
		if err != nil {
			return nil, nil, 0, err
		}

		response := &gitalypb.TreeEntryResponse{
			Type: gitalypb.TreeEntryResponse_TREE,
			Oid:  treeEntry.Oid,
			Size: treeInfo.Size,
			Mode: treeEntry.Mode,
		}
		return response, nil, 0, nil
	}

	objectInfo, err := c.Info(treeEntry.Oid)
	if err != nil {
		return nil, nil, 0, status.Errorf(codes.Internal, "TreeEntry: %v", err)
	}

	if strings.ToLower(treeEntry.Type.String()) != objectInfo.Type {
		return nil, nil, 0, status.Errorf(
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
		return response, nil, 0, nil
	}

	blobReader, err := c.Blob(objectInfo.Oid)
	if err != nil {
		return nil, nil, 0, err
	}

	return response, blobReader, dataLength, nil
}

func findTreeEntryStreamWriter(stream gitalypb.CommitService_FindTreeEntriesServer, response *gitalypb.TreeEntryResponse, blobReader io.Reader) io.Writer {
	return streamio.NewWriter(func(p []byte) error {
		response.Data = p

		if err := stream.Send(response); err != nil {
			return status.Errorf(codes.Unavailable, "TreeEntry: send: %v", err)
		}

		// Use a new response so we don't send other fields (Size, ...) over and over
		response = &gitalypb.TreeEntryResponse{}

		return nil
	})
}

func findTreeEntriesStreamWriter(stream gitalypb.CommitService_FindTreeEntriesServer, response *gitalypb.TreeEntryResponse, blobReader io.Reader) io.Writer {
	return streamio.NewWriter(func(p []byte) error {
		response.Data = p

		if err := stream.Send(response); err != nil {
			return status.Errorf(codes.Unavailable, "TreeEntry: send: %v", err)
		}

		// Use a new response so we don't send other fields (Size, ...) over and over
		response = &gitalypb.TreeEntryResponse{}

		return nil
	})
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

	return findAndSendTreeEntry(stream, c, string(in.GetRevision()), requestPath, in.GetLimit())
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

func (s *server) FindTreeEntries(in *gitalypb.FindTreeEntriesRequest, stream gitalypb.CommitService_FindTreeEntriesServer) error {
	if err := validateFindTreeEntriesRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	c, err := catfile.New(stream.Context(), in.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := findAndSendTreeEntries(stream, c, string(in.GetRevision()), in.GetPaths(), in.GetLimit()); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func validateFindTreeEntriesRequest(in *gitalypb.FindTreeEntriesRequest) error {
	if err := git.ValidateRevision(in.GetRevision()); err != nil {
		return err
	}

	if len(in.GetPaths()) == 0 {
		return fmt.Errorf("empty Path")
	}

	return nil
}
