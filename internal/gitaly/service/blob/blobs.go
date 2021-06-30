package blob

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
)

func verifyListBlobsRequest(req *gitalypb.ListBlobsRequest) error {
	if req.GetRepository() == nil {
		return errors.New("empty repository")
	}
	if len(req.GetRevisions()) == 0 {
		return errors.New("missing revisions")
	}
	for _, revision := range req.Revisions {
		if strings.HasPrefix(revision, "-") && revision != "--all" && revision != "--not" {
			return fmt.Errorf("invalid revision: %q", revision)
		}
	}
	return nil
}

// ListBlobs finds all blobs which are transitively reachable via a graph walk of the given set of
// revisions.
func (s *server) ListBlobs(req *gitalypb.ListBlobsRequest, stream gitalypb.BlobService_ListBlobsServer) error {
	if err := verifyListBlobsRequest(req); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	ctx := stream.Context()
	repo := s.localrepo(req.GetRepository())

	catfileProcess, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return helper.ErrInternal(fmt.Errorf("creating catfile process: %w", err))
	}

	chunker := chunk.New(&blobSender{
		send: func(blobs []*gitalypb.ListBlobsResponse_Blob) error {
			return stream.Send(&gitalypb.ListBlobsResponse{
				Blobs: blobs,
			})
		},
	})

	gitVersion, err := git.CurrentVersion(ctx, s.gitCmdFactory)
	if err != nil {
		return helper.ErrInternalf("cannot determine Git version: %v", err)
	}

	var revlistOptions []gitpipe.RevlistOption
	if gitVersion.SupportsObjectTypeFilter() {
		revlistOptions = append(revlistOptions, gitpipe.WithObjectTypeFilter(gitpipe.ObjectTypeBlob))
	}

	revlistIter := gitpipe.Revlist(ctx, repo, req.GetRevisions(), revlistOptions...)
	catfileInfoIter := gitpipe.CatfileInfo(ctx, catfileProcess, revlistIter)
	catfileInfoIter = gitpipe.CatfileInfoFilter(ctx, catfileInfoIter, func(r gitpipe.CatfileInfoResult) bool {
		return r.ObjectInfo.Type == "blob"
	})

	// If we have a zero bytes limit, then the caller didn't request any blob contents at all.
	// We can thus skip reading blob contents completely.
	if req.GetBytesLimit() == 0 {
		var i uint32
		for catfileInfoIter.Next() {
			blob := catfileInfoIter.Result()

			if err := chunker.Send(&gitalypb.ListBlobsResponse_Blob{
				Oid:  blob.ObjectInfo.Oid.String(),
				Size: blob.ObjectInfo.Size,
			}); err != nil {
				return helper.ErrInternal(fmt.Errorf("sending blob chunk: %w", err))
			}

			i++
			if req.GetLimit() > 0 && i >= req.GetLimit() {
				break
			}
		}

		if err := catfileInfoIter.Err(); err != nil {
			return helper.ErrInternal(err)
		}
	} else {
		catfileObjectIter := gitpipe.CatfileObject(ctx, catfileProcess, catfileInfoIter)

		var i uint32
		for catfileObjectIter.Next() {
			blob := catfileObjectIter.Result()

			headerSent := false
			dataChunker := streamio.NewWriter(func(p []byte) error {
				message := &gitalypb.ListBlobsResponse_Blob{
					Data: p,
				}

				if !headerSent {
					message.Oid = blob.ObjectInfo.Oid.String()
					message.Size = blob.ObjectInfo.Size
					headerSent = true
				}

				if err := chunker.Send(message); err != nil {
					return helper.ErrInternal(fmt.Errorf("sending blob chunk: %w", err))
				}

				return nil
			})

			readLimit := req.GetBytesLimit()
			if readLimit < 0 {
				readLimit = blob.ObjectInfo.Size
			}

			_, err := io.CopyN(dataChunker, blob.ObjectReader, readLimit)
			if err != nil && !errors.Is(err, io.EOF) {
				return helper.ErrInternal(fmt.Errorf("sending blob data: %w", err))
			}

			// Discard trailing blob data in case the blob is bigger than the read
			// limit.
			_, err = io.Copy(ioutil.Discard, blob.ObjectReader)
			if err != nil {
				return helper.ErrInternal(fmt.Errorf("discarding blob data: %w", err))
			}

			// If we still didn't send any header, then it probably means that the blob
			// itself didn't contain any data. Let's be prepared and send out the blob
			// header manually in that case.
			if !headerSent {
				if err := chunker.Send(&gitalypb.ListBlobsResponse_Blob{
					Oid:  blob.ObjectInfo.Oid.String(),
					Size: blob.ObjectInfo.Size,
				}); err != nil {
					return helper.ErrInternal(fmt.Errorf("sending blob chunk: %w", err))
				}
			}

			i++
			if req.GetLimit() > 0 && i >= req.GetLimit() {
				break
			}
		}

		if err := catfileObjectIter.Err(); err != nil {
			return helper.ErrInternal(err)
		}
	}

	if err := chunker.Flush(); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

type blobSender struct {
	blobs []*gitalypb.ListBlobsResponse_Blob
	send  func([]*gitalypb.ListBlobsResponse_Blob) error
}

func (t *blobSender) Reset() {
	t.blobs = t.blobs[:0]
}

func (t *blobSender) Append(m proto.Message) {
	t.blobs = append(t.blobs, m.(*gitalypb.ListBlobsResponse_Blob))
}

func (t *blobSender) Send() error {
	return t.send(t.blobs)
}
