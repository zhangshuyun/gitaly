package commit

import (
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func sendTreeEntry(stream pb.CommitService_TreeEntryServer, c *catfile.Batch, revision, path string, limit int64) error {
	treeEntry, err := TreeEntryForRevisionAndPath(c, revision, path)
	if err != nil {
		return err
	}

	if treeEntry == nil || len(treeEntry.Oid) == 0 {
		return helper.DecorateError(codes.Unavailable, stream.Send(&pb.TreeEntryResponse{}))
	}

	if treeEntry.Type == pb.TreeEntry_COMMIT {
		response := &pb.TreeEntryResponse{
			Type: pb.TreeEntryResponse_COMMIT,
			Mode: treeEntry.Mode,
			Oid:  treeEntry.Oid,
		}
		if err := stream.Send(response); err != nil {
			return status.Errorf(codes.Unavailable, "TreeEntry: send: %v", err)
		}

		return nil
	}

	if treeEntry.Type == pb.TreeEntry_TREE {
		treeInfo, err := c.Info(treeEntry.Oid)
		if err != nil {
			return err
		}

		response := &pb.TreeEntryResponse{
			Type: pb.TreeEntryResponse_TREE,
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

	response := &pb.TreeEntryResponse{
		Type: pb.TreeEntryResponse_BLOB,
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
		response = &pb.TreeEntryResponse{}

		return nil
	})

	_, err = io.CopyN(sw, blobReader, dataLength)
	return err
}

func (s *server) TreeEntry(in *pb.TreeEntryRequest, stream pb.CommitService_TreeEntryServer) error {
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

func validateRequest(in *pb.TreeEntryRequest) error {
	if len(in.GetRevision()) == 0 {
		return fmt.Errorf("empty Revision")
	}

	if len(in.GetPath()) == 0 {
		return fmt.Errorf("empty Path")
	}

	return nil
}
