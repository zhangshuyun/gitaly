package blob

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/golang/protobuf/proto"
	gitaly_errors "gitlab.com/gitlab-org/gitaly/v14/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"golang.org/x/text/transform"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// lfsPointerMaxSize is the maximum size for an lfs pointer text blob. This limit is used
	// as a heuristic to filter blobs which can't be LFS pointers. The format of these pointers
	// is described in https://github.com/git-lfs/git-lfs/blob/master/docs/spec.md#the-pointer.
	lfsPointerMaxSize = 200
)

var (
	errLimitReached = errors.New("limit reached")
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

	if featureflag.IsDisabled(ctx, featureflag.LFSPointersPipeline) {
		if err := findLFSPointersByRevisions(ctx, repo, s.gitCmdFactory, chunker, int(in.Limit), in.Revisions...); err != nil {
			if !errors.Is(err, errLimitReached) {
				return err
			}
		}
	} else {
		catfileProcess, err := s.catfileCache.BatchProcess(ctx, repo)
		if err != nil {
			return helper.ErrInternal(fmt.Errorf("creating catfile process: %w", err))
		}

		revlistChan := revlist(ctx, repo, in.GetRevisions(), withBlobLimit(lfsPointerMaxSize))
		catfileInfoChan := catfileInfo(ctx, catfileProcess, revlistChan)
		catfileInfoChan = catfileInfoFilter(ctx, catfileInfoChan, func(r catfileInfoResult) bool {
			return r.objectInfo.Type == "blob" && r.objectInfo.Size <= lfsPointerMaxSize
		})
		catfileObjectChan := catfileObject(ctx, catfileProcess, catfileInfoChan)
		catfileObjectChan = catfileObjectFilter(ctx, catfileObjectChan, func(r catfileObjectResult) bool {
			return git.IsLFSPointer(r.objectData)
		})

		if err := sendLFSPointers(chunker, catfileObjectChan, int(in.Limit)); err != nil {
			return err
		}
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

	if featureflag.IsDisabled(ctx, featureflag.LFSPointersPipeline) {
		cmd, err := repo.Exec(ctx, git.SubCmd{
			Name: "cat-file",
			Flags: []git.Option{
				git.Flag{Name: "--batch-all-objects"},
				git.Flag{Name: "--batch-check=%(objecttype) %(objectsize) %(objectname)"},
				git.Flag{Name: "--buffer"},
				git.Flag{Name: "--unordered"},
			},
		})
		if err != nil {
			return status.Errorf(codes.Internal, "could not run batch-check: %v", err)
		}

		filteredReader := transform.NewReader(cmd, blobFilter{
			maxSize: lfsPointerMaxSize,
		})

		if err := readLFSPointers(ctx, repo, chunker, filteredReader, int(in.Limit)); err != nil {
			if !errors.Is(err, errLimitReached) {
				return status.Errorf(codes.Internal, "could not read LFS pointers: %v", err)
			}
		}
	} else {
		catfileProcess, err := s.catfileCache.BatchProcess(ctx, repo)
		if err != nil {
			return helper.ErrInternal(fmt.Errorf("creating catfile process: %w", err))
		}

		catfileInfoChan := catfileInfoAllObjects(ctx, repo)
		catfileInfoChan = catfileInfoFilter(ctx, catfileInfoChan, func(r catfileInfoResult) bool {
			return r.objectInfo.Type == "blob" && r.objectInfo.Size <= lfsPointerMaxSize
		})
		catfileObjectChan := catfileObject(ctx, catfileProcess, catfileInfoChan)
		catfileObjectChan = catfileObjectFilter(ctx, catfileObjectChan, func(r catfileObjectResult) bool {
			return git.IsLFSPointer(r.objectData)
		})

		if err := sendLFSPointers(chunker, catfileObjectChan, int(in.Limit)); err != nil {
			return err
		}
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

	if featureflag.IsDisabled(ctx, featureflag.LFSPointersPipeline) {
		objectIDs := strings.Join(req.BlobIds, "\n")

		if err := readLFSPointers(ctx, repo, chunker, strings.NewReader(objectIDs), 0); err != nil {
			if !errors.Is(err, errLimitReached) {
				return err
			}
		}
	} else {
		catfileProcess, err := s.catfileCache.BatchProcess(ctx, repo)
		if err != nil {
			return helper.ErrInternal(fmt.Errorf("creating catfile process: %w", err))
		}

		objectChan := make(chan revlistResult, len(req.GetBlobIds()))
		for _, blobID := range req.GetBlobIds() {
			objectChan <- revlistResult{oid: git.ObjectID(blobID)}
		}
		close(objectChan)

		catfileInfoChan := catfileInfo(ctx, catfileProcess, objectChan)
		catfileInfoChan = catfileInfoFilter(ctx, catfileInfoChan, func(r catfileInfoResult) bool {
			return r.objectInfo.Type == "blob" && r.objectInfo.Size <= lfsPointerMaxSize
		})
		catfileObjectChan := catfileObject(ctx, catfileProcess, catfileInfoChan)
		catfileObjectChan = catfileObjectFilter(ctx, catfileObjectChan, func(r catfileObjectResult) bool {
			return git.IsLFSPointer(r.objectData)
		})

		if err := sendLFSPointers(chunker, catfileObjectChan, 0); err != nil {
			return err
		}
	}

	return nil
}

func validateGetLFSPointersRequest(req *gitalypb.GetLFSPointersRequest) error {
	if req.GetRepository() == nil {
		return gitaly_errors.ErrEmptyRepository
	}

	if len(req.GetBlobIds()) == 0 {
		return fmt.Errorf("empty BlobIds")
	}

	return nil
}

// findLFSPointersByRevisions will return all LFS objects reachable via the given set of revisions.
// Revisions accept all syntax supported by git-rev-list(1).
func findLFSPointersByRevisions(
	ctx context.Context,
	repo *localrepo.Repo,
	gitCmdFactory git.CommandFactory,
	chunker *chunk.Chunker,
	limit int,
	revisions ...string,
) (returnErr error) {
	// git-rev-list(1) currently does not have any way to list all reachable objects of a
	// certain type.
	var revListStderr bytes.Buffer
	revlist, err := repo.Exec(ctx, git.SubCmd{
		Name: "rev-list",
		Flags: []git.Option{
			git.Flag{Name: "--in-commit-order"},
			git.Flag{Name: "--objects"},
			git.Flag{Name: "--no-object-names"},
			git.Flag{Name: fmt.Sprintf("--filter=blob:limit=%d", lfsPointerMaxSize)},
		},
		Args: revisions,
	}, git.WithStderr(&revListStderr))
	if err != nil {
		return fmt.Errorf("could not execute rev-list: %w", err)
	}

	defer func() {
		// There is no way to properly determine whether the process has exited because of
		// us signalling the context or because of any other means. We can only approximate
		// this by checking whether the process state is "signal: killed". Which again is
		// awful, but given that `Signaled()` status is also not accessible to us,
		// it's the best we could do.
		//
		// Let's not do any of this, it's awful. Instead, we can simply check whether we
		// have reached the limit. If so, we found all LFS pointers which the user requested
		// and needn't bother whether git-rev-list(1) may have failed. So let's instead just
		// have the RPCcontext cancel the process.
		if errors.Is(returnErr, errLimitReached) {
			return
		}

		if err := revlist.Wait(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("rev-list failed: %w, stderr: %q",
				err, revListStderr.String())
		}
	}()

	return readLFSPointers(ctx, repo, chunker, revlist, limit)
}

// readLFSPointers reads object IDs of potential LFS pointers from the given reader and for each of
// them, it will determine whether the referenced object is an LFS pointer. Objects which are not a
// valid LFS pointer will be ignored. Objects which do not exist result in an error.
func readLFSPointers(
	ctx context.Context,
	repo *localrepo.Repo,
	chunker *chunk.Chunker,
	objectIDReader io.Reader,
	limit int,
) (returnErr error) {
	defer func() {
		if err := chunker.Flush(); err != nil && returnErr == nil {
			returnErr = err
		}
	}()

	catfileBatch, err := repo.Exec(ctx, git.SubCmd{
		Name: "cat-file",
		Flags: []git.Option{
			git.Flag{Name: "--batch"},
			git.Flag{Name: "--buffer"},
		},
	}, git.WithStdin(objectIDReader))
	if err != nil {
		return fmt.Errorf("could not execute cat-file: %w", err)
	}

	var pointersFound int
	reader := bufio.NewReader(catfileBatch)
	buf := &bytes.Buffer{}
	for {
		objectInfo, err := catfile.ParseObjectInfo(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("could not get LFS pointer info: %w", err)
		}

		// Avoid allocating bytes for an LFS pointer until we know the current
		// blob really is an LFS pointer.
		buf.Reset()
		if _, err := io.CopyN(buf, reader, objectInfo.Size+1); err != nil {
			return fmt.Errorf("could not read LFS pointer candidate: %w", err)
		}
		tempData := buf.Bytes()[:buf.Len()-1]

		if objectInfo.Type != "blob" || !git.IsLFSPointer(tempData) {
			continue
		}

		// Now that we know this is an LFS pointer it is not a waste to allocate
		// memory.
		data := make([]byte, len(tempData))
		copy(data, tempData)

		if err := chunker.Send(&gitalypb.LFSPointer{
			Data: data,
			Size: int64(len(data)),
			Oid:  objectInfo.Oid.String(),
		}); err != nil {
			return fmt.Errorf("sending LFS pointer chunk: %w", err)
		}

		pointersFound++

		// Exit early in case we've got all LFS pointers. We want to do this here instead of
		// just terminating the loop because we need to check git-cat-file(1)'s exit code in
		// case the loop finishes successfully via an EOF. We don't want to do so here
		// though: we don't care for successful termination of the command, we only care
		// that we've got all pointers. The command is then getting cancelled via the
		// parent's context.
		if limit > 0 && pointersFound >= limit {
			return errLimitReached
		}
	}

	if err := catfileBatch.Wait(); err != nil {
		return nil
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

func sendLFSPointers(chunker *chunk.Chunker, lfsPointers <-chan catfileObjectResult, limit int) error {
	var i int
	for lfsPointer := range lfsPointers {
		if lfsPointer.err != nil {
			return helper.ErrInternal(lfsPointer.err)
		}

		if err := chunker.Send(&gitalypb.LFSPointer{
			Data: lfsPointer.objectData,
			Size: lfsPointer.objectInfo.Size,
			Oid:  lfsPointer.objectInfo.Oid.String(),
		}); err != nil {
			return helper.ErrInternal(fmt.Errorf("sending LFS pointer chunk: %w", err))
		}

		i++
		if limit > 0 && i >= limit {
			break
		}
	}

	if err := chunker.Flush(); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}
