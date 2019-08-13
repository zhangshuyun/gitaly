package blob

import (
	"io"
	"io/ioutil"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/service/commit"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func sendGetBlobsResponse(req *gitalypb.GetBlobsRequest, stream gitalypb.BlobService_GetBlobsServer, c *catfile.Batch) error {
	tef := commit.NewTreeEntryFinder(c)

	for _, revisionPath := range req.RevisionPaths {
		revision := revisionPath.Revision
		path := revisionPath.Path

		treeEntry, err := tef.FindByRevisionAndPath(revision, string(path))
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

			if err := stream.Send(response); err != nil {
				return status.Errorf(codes.Unavailable, "GetBlobs: send: %v", err)
			}

			continue
		}

		objectInfo, err := c.Info(treeEntry.Oid)
		if err != nil {
			return status.Errorf(codes.Internal, "GetBlobs: %v", err)
		}
		if objectInfo.Type != "blob" {
			return status.Errorf(codes.InvalidArgument, "GetBlobs: object at %s:%s is %s, not blob", revision, path, objectInfo.Type)
		}

		response.Size = objectInfo.Size

		var readLimit int64
		if req.Limit < 0 || req.Limit > objectInfo.Size {
			readLimit = objectInfo.Size
		} else {
			readLimit = req.Limit
		}

		// For correctness it does not matter, but for performance, the order is
		// important: first check if readlimit == 0, if not, only then create
		// blobReader.
		if readLimit == 0 {
			if err := stream.Send(response); err != nil {
				return status.Errorf(codes.Unavailable, "GetBlobs: send: %v", err)
			}
			continue
		}

		blobReader, err := c.Blob(objectInfo.Oid)
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

		_, err = io.CopyN(sw, blobReader, readLimit)
		if err != nil {
			return status.Errorf(codes.Unavailable, "GetBlobs: send: %v", err)
		}

		if _, err := io.Copy(ioutil.Discard, blobReader); err != nil {
			return status.Errorf(codes.Unavailable, "GetBlobs: discarding data: %v", err)
		}
	}

	return nil
}

func (*server) GetBlobs(req *gitalypb.GetBlobsRequest, stream gitalypb.BlobService_GetBlobsServer) error {
	if err := validateGetBlobsRequest(req); err != nil {
		return err
	}

	c, err := catfile.New(stream.Context(), req.Repository)
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
