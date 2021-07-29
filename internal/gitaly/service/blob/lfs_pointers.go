package blob

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v14/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// lfsPointerMaxSize is the maximum size for an lfs pointer text blob. This limit is used
	// as a heuristic to filter blobs which can't be LFS pointers. The format of these pointers
	// is described in https://github.com/git-lfs/git-lfs/blob/master/docs/spec.md#the-pointer.
	lfsPointerMaxSize = 200
)

// ListLFSPointers finds all LFS pointers which are transitively reachable via a graph walk of the
// given set of revisions.
func (s *server) ListLFSPointers(in *gitalypb.ListLFSPointersRequest, stream gitalypb.BlobService_ListLFSPointersServer) error {
	ctx := stream.Context()

	if in.GetRepository() == nil {
		return status.Error(codes.InvalidArgument, "empty repository")
	}
	if len(in.Revisions) == 0 {
		return status.Error(codes.InvalidArgument, "missing revisions")
	}
	for _, revision := range in.Revisions {
		if strings.HasPrefix(revision, "-") && revision != "--all" && revision != "--not" {
			return status.Errorf(codes.InvalidArgument, "invalid revision: %q", revision)
		}
	}

	chunker := chunk.New(&lfsPointerSender{
		send: func(pointers []*gitalypb.LFSPointer) error {
			return stream.Send(&gitalypb.ListLFSPointersResponse{
				LfsPointers: pointers,
			})
		},
	})

	repo := s.localrepo(in.GetRepository())

	catfileProcess, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return helper.ErrInternal(fmt.Errorf("creating catfile process: %w", err))
	}

	gitVersion, err := git.CurrentVersion(ctx, s.gitCmdFactory)
	if err != nil {
		return helper.ErrInternalf("cannot determine Git version: %v", err)
	}

	revlistOptions := []gitpipe.RevlistOption{
		gitpipe.WithObjects(),
		gitpipe.WithBlobLimit(lfsPointerMaxSize),
	}
	if gitVersion.SupportsObjectTypeFilter() {
		revlistOptions = append(revlistOptions, gitpipe.WithObjectTypeFilter(gitpipe.ObjectTypeBlob))
	}

	revlistIter := gitpipe.Revlist(ctx, repo, in.GetRevisions(), revlistOptions...)
	catfileInfoIter := gitpipe.CatfileInfo(ctx, catfileProcess, revlistIter)
	catfileInfoIter = gitpipe.CatfileInfoFilter(ctx, catfileInfoIter, func(r gitpipe.CatfileInfoResult) bool {
		return r.ObjectInfo.Type == "blob" && r.ObjectInfo.Size <= lfsPointerMaxSize
	})
	catfileObjectIter := gitpipe.CatfileObject(ctx, catfileProcess, catfileInfoIter)

	if err := sendLFSPointers(chunker, catfileObjectIter, int(in.Limit)); err != nil {
		return err
	}

	return nil
}

// ListAllLFSPointers finds all LFS pointers which exist in the repository, including those which
// are not reachable via graph walks.
func (s *server) ListAllLFSPointers(in *gitalypb.ListAllLFSPointersRequest, stream gitalypb.BlobService_ListAllLFSPointersServer) error {
	ctx := stream.Context()

	if in.GetRepository() == nil {
		return status.Error(codes.InvalidArgument, "empty repository")
	}

	repo := s.localrepo(in.GetRepository())

	chunker := chunk.New(&lfsPointerSender{
		send: func(pointers []*gitalypb.LFSPointer) error {
			return stream.Send(&gitalypb.ListAllLFSPointersResponse{
				LfsPointers: pointers,
			})
		},
	})

	catfileProcess, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return helper.ErrInternal(fmt.Errorf("creating catfile process: %w", err))
	}

	catfileInfoIter := gitpipe.CatfileInfoAllObjects(ctx, repo)
	catfileInfoIter = gitpipe.CatfileInfoFilter(ctx, catfileInfoIter, func(r gitpipe.CatfileInfoResult) bool {
		return r.ObjectInfo.Type == "blob" && r.ObjectInfo.Size <= lfsPointerMaxSize
	})
	catfileObjectIter := gitpipe.CatfileObject(ctx, catfileProcess, catfileInfoIter)

	if err := sendLFSPointers(chunker, catfileObjectIter, int(in.Limit)); err != nil {
		return err
	}

	return nil
}

// GetLFSPointers takes the list of requested blob IDs and filters them down to blobs which are
// valid LFS pointers. It is fine to pass blob IDs which do not point to a valid LFS pointer, but
// passing blob IDs which do not exist results in an error.
func (s *server) GetLFSPointers(req *gitalypb.GetLFSPointersRequest, stream gitalypb.BlobService_GetLFSPointersServer) error {
	ctx := stream.Context()

	if err := validateGetLFSPointersRequest(req); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetLFSPointers: %v", err)
	}

	repo := s.localrepo(req.GetRepository())

	chunker := chunk.New(&lfsPointerSender{
		send: func(pointers []*gitalypb.LFSPointer) error {
			return stream.Send(&gitalypb.GetLFSPointersResponse{
				LfsPointers: pointers,
			})
		},
	})

	catfileProcess, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return helper.ErrInternal(fmt.Errorf("creating catfile process: %w", err))
	}

	blobs := make([]gitpipe.RevisionResult, len(req.GetBlobIds()))
	for i, blobID := range req.GetBlobIds() {
		blobs[i] = gitpipe.RevisionResult{OID: git.ObjectID(blobID)}
	}

	catfileInfoIter := gitpipe.CatfileInfo(ctx, catfileProcess, gitpipe.NewRevisionIterator(blobs))
	catfileInfoIter = gitpipe.CatfileInfoFilter(ctx, catfileInfoIter, func(r gitpipe.CatfileInfoResult) bool {
		return r.ObjectInfo.Type == "blob" && r.ObjectInfo.Size <= lfsPointerMaxSize
	})
	catfileObjectIter := gitpipe.CatfileObject(ctx, catfileProcess, catfileInfoIter)

	if err := sendLFSPointers(chunker, catfileObjectIter, 0); err != nil {
		return err
	}

	return nil
}

func validateGetLFSPointersRequest(req *gitalypb.GetLFSPointersRequest) error {
	if req.GetRepository() == nil {
		return gitalyerrors.ErrEmptyRepository
	}

	if len(req.GetBlobIds()) == 0 {
		return fmt.Errorf("empty BlobIds")
	}

	return nil
}

type lfsPointerSender struct {
	pointers []*gitalypb.LFSPointer
	send     func([]*gitalypb.LFSPointer) error
}

func (t *lfsPointerSender) Reset() {
	t.pointers = t.pointers[:0]
}

func (t *lfsPointerSender) Append(m proto.Message) {
	t.pointers = append(t.pointers, m.(*gitalypb.LFSPointer))
}

func (t *lfsPointerSender) Send() error {
	return t.send(t.pointers)
}

func sendLFSPointers(chunker *chunk.Chunker, iter gitpipe.CatfileObjectIterator, limit int) error {
	buffer := bytes.NewBuffer(make([]byte, 0, lfsPointerMaxSize))

	var i int
	for iter.Next() {
		lfsPointer := iter.Result()

		// Avoid allocating bytes for an LFS pointer until we know the current blob really
		// is an LFS pointer.
		buffer.Reset()

		// Given that we filter pipeline objects by size, the biggest object we may see here
		// is 200 bytes in size. So it's not much of a problem to read this into memory
		// completely.
		if _, err := io.Copy(buffer, lfsPointer.ObjectReader); err != nil {
			return helper.ErrInternal(fmt.Errorf("reading LFS pointer data: %w", err))
		}

		if !git.IsLFSPointer(buffer.Bytes()) {
			continue
		}

		objectData := make([]byte, buffer.Len())
		copy(objectData, buffer.Bytes())

		if err := chunker.Send(&gitalypb.LFSPointer{
			Data: objectData,
			Size: int64(len(objectData)),
			Oid:  lfsPointer.ObjectInfo.Oid.String(),
		}); err != nil {
			return helper.ErrInternal(fmt.Errorf("sending LFS pointer chunk: %w", err))
		}

		i++
		if limit > 0 && i >= limit {
			break
		}
	}

	if err := iter.Err(); err != nil {
		return helper.ErrInternal(err)
	}

	if err := chunker.Flush(); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}
