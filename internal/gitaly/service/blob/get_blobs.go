package blob

import (
	"bytes"
	"io"
	"io/ioutil"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var treeEntryToObjectType = map[gitalypb.TreeEntry_EntryType]gitalypb.ObjectType{
	gitalypb.TreeEntry_BLOB:   gitalypb.ObjectType_BLOB,
	gitalypb.TreeEntry_TREE:   gitalypb.ObjectType_TREE,
	gitalypb.TreeEntry_COMMIT: gitalypb.ObjectType_COMMIT}

func sendGetBlobsResponse(req *gitalypb.GetBlobsRequest, stream gitalypb.BlobService_GetBlobsServer, c catfile.Batch) error {
	ctx := stream.Context()

	tef := commit.NewTreeEntryFinder(c)

	for _, revisionPath := range req.RevisionPaths {
		revision := revisionPath.Revision
		path := revisionPath.Path

		if len(path) > 1 {
			path = bytes.TrimRight(path, "/")
		}

		treeEntry, err := tef.FindByRevisionAndPath(ctx, revision, string(path))
		if err != nil {
			return err
		}

		response := &gitalypb.GetBlobsResponse{Revision: revision, Path: path}

		if treeEntry == nil || len(treeEntry.Oid) == 0 {
			if err := stream.Send(response); err != nil {
				return status.Errorf(codes.Unavailable, "GetBlobs: send: %v", err)
			}

			continue
		}

		response.Mode = treeEntry.Mode
		response.Oid = treeEntry.Oid

		if treeEntry.Type == gitalypb.TreeEntry_COMMIT {
			response.IsSubmodule = true
			response.Type = gitalypb.ObjectType_COMMIT

			if err := stream.Send(response); err != nil {
				return status.Errorf(codes.Unavailable, "GetBlobs: send: %v", err)
			}

			continue
		}

		objectInfo, err := c.Info(ctx, git.Revision(treeEntry.Oid))
		if err != nil {
			return status.Errorf(codes.Internal, "GetBlobs: %v", err)
		}

		response.Size = objectInfo.Size

		var ok bool
		response.Type, ok = treeEntryToObjectType[treeEntry.Type]

		if !ok {
			continue
		}

		if response.Type != gitalypb.ObjectType_BLOB {
			if err := stream.Send(response); err != nil {
				return status.Errorf(codes.Unavailable, "GetBlobs: send: %v", err)
			}
			continue
		}

		if err = sendBlobTreeEntry(response, stream, c, req.GetLimit()); err != nil {
			return err
		}
	}

	return nil
}

func sendBlobTreeEntry(response *gitalypb.GetBlobsResponse, stream gitalypb.BlobService_GetBlobsServer, c catfile.Batch, limit int64) error {
	ctx := stream.Context()

	var readLimit int64
	if limit < 0 || limit > response.Size {
		readLimit = response.Size
	} else {
		readLimit = limit
	}

	// For correctness it does not matter, but for performance, the order is
	// important: first check if readlimit == 0, if not, only then create
	// blobObj.
	if readLimit == 0 {
		if err := stream.Send(response); err != nil {
			return status.Errorf(codes.Unavailable, "GetBlobs: send: %v", err)
		}
		return nil
	}

	blobObj, err := c.Blob(ctx, git.Revision(response.Oid))
	if err != nil {
		return status.Errorf(codes.Internal, "GetBlobs: %v", err)
	}

	sw := streamio.NewWriter(func(p []byte) error {
		msg := &gitalypb.GetBlobsResponse{}
		if response != nil {
			msg = response
			response = nil
		}

		msg.Data = p

		return stream.Send(msg)
	})

	_, err = io.CopyN(sw, blobObj.Reader, readLimit)
	if err != nil {
		return status.Errorf(codes.Unavailable, "GetBlobs: send: %v", err)
	}

	if _, err := io.Copy(ioutil.Discard, blobObj.Reader); err != nil {
		return status.Errorf(codes.Unavailable, "GetBlobs: discarding data: %v", err)
	}

	return nil
}

func (s *server) GetBlobs(req *gitalypb.GetBlobsRequest, stream gitalypb.BlobService_GetBlobsServer) error {
	if err := validateGetBlobsRequest(req); err != nil {
		return err
	}

	repo := s.localrepo(req.GetRepository())

	c, err := s.catfileCache.BatchProcess(stream.Context(), repo)
	if err != nil {
		return err
	}

	return sendGetBlobsResponse(req, stream, c)
}

func validateGetBlobsRequest(req *gitalypb.GetBlobsRequest) error {
	if req.Repository == nil {
		return status.Errorf(codes.InvalidArgument, "GetBlobs: empty Repository")
	}

	if len(req.RevisionPaths) == 0 {
		return status.Errorf(codes.InvalidArgument, "GetBlobs: empty RevisionPaths")
	}

	for _, rp := range req.RevisionPaths {
		if err := git.ValidateRevision([]byte(rp.Revision)); err != nil {
			return status.Errorf(codes.InvalidArgument, "GetBlobs: %v", err)
		}
	}

	return nil
}
