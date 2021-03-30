package blob

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	gitaly_errors "gitlab.com/gitlab-org/gitaly/internal/errors"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"golang.org/x/text/transform"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// lfsPointerMaxSize is the maximum size for an lfs pointer text blob. This limit is used
	// as a heuristic to filter blobs which can't be LFS pointers. The format of these pointers
	// is described in https://github.com/git-lfs/git-lfs/blob/master/docs/spec.md#the-pointer.
	lfsPointerMaxSize = 200

	// lfsPointerSliceSize is the maximum number of LFSPointers to send at once.
	lfsPointerSliceSize = 100
)

var (
	errInvalidRevision = errors.New("invalid revision")
)

type getLFSPointerByRevisionRequest interface {
	GetRepository() *gitalypb.Repository
	GetRevision() []byte
}

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

	repo := localrepo.New(s.gitCmdFactory, in.Repository, s.cfg)
	lfsPointers, err := findLFSPointersByRevisions(ctx, repo, s.gitCmdFactory, int(in.Limit), in.Revisions...)
	if err != nil {
		if errors.Is(err, errInvalidRevision) {
			return status.Errorf(codes.InvalidArgument, err.Error())
		}
		return err
	}

	if err := sliceLFSPointers(lfsPointers, func(slice []*gitalypb.LFSPointer) error {
		return stream.Send(&gitalypb.ListLFSPointersResponse{
			LfsPointers: slice,
		})
	}); err != nil {
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

	repo := localrepo.New(s.gitCmdFactory, in.Repository, s.cfg)
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

	filteredReader := transform.NewReader(cmd, lfsPointerFilter{})
	lfsPointers, err := readLFSPointers(ctx, repo, filteredReader, int(in.Limit))
	if err != nil {
		return status.Errorf(codes.Internal, "could not read LFS pointers: %v", err)
	}

	if err := sliceLFSPointers(lfsPointers, func(slice []*gitalypb.LFSPointer) error {
		return stream.Send(&gitalypb.ListAllLFSPointersResponse{
			LfsPointers: slice,
		})
	}); err != nil {
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

	repo := localrepo.New(s.gitCmdFactory, req.Repository, s.cfg)
	objectIDs := strings.Join(req.BlobIds, "\n")

	lfsPointers, err := readLFSPointers(ctx, repo, strings.NewReader(objectIDs), 0)
	if err != nil {
		return err
	}

	err = sliceLFSPointers(lfsPointers, func(slice []*gitalypb.LFSPointer) error {
		return stream.Send(&gitalypb.GetLFSPointersResponse{
			LfsPointers: slice,
		})
	})
	if err != nil {
		return err
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

// GetNewLFSPointers returns all LFS pointers which were newly introduced in a given revision,
// excluding either all other existing refs or a set of provided refs. If NotInAll is set, then it
// has precedence over NotInRefs.
func (s *server) GetNewLFSPointers(in *gitalypb.GetNewLFSPointersRequest, stream gitalypb.BlobService_GetNewLFSPointersServer) error {
	ctx := stream.Context()

	if err := validateGetLfsPointersByRevisionRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetNewLFSPointers: %v", err)
	}

	repo := localrepo.New(s.gitCmdFactory, in.Repository, s.cfg)

	var refs []string
	if in.NotInAll {
		refs = []string{string(in.Revision), "--not", "--all"}
	} else {
		refs = []string{string(in.Revision), "--not"}
		for _, notInRef := range in.NotInRefs {
			refs = append(refs, string(notInRef))
		}
	}

	lfsPointers, err := findLFSPointersByRevisions(ctx, repo, s.gitCmdFactory, int(in.Limit), refs...)
	if err != nil {
		if errors.Is(err, errInvalidRevision) {
			return status.Errorf(codes.InvalidArgument, err.Error())
		}
		return err
	}

	err = sliceLFSPointers(lfsPointers, func(slice []*gitalypb.LFSPointer) error {
		return stream.Send(&gitalypb.GetNewLFSPointersResponse{
			LfsPointers: slice,
		})
	})
	if err != nil {
		return err
	}

	return nil
}

func validateGetLfsPointersByRevisionRequest(in getLFSPointerByRevisionRequest) error {
	if in.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	return git.ValidateRevision(in.GetRevision())
}

// GetAllLFSPointers returns all LFS pointers of the git repository which are reachable by any git
// reference. LFS pointers are streamed back in batches of lfsPointerSliceSize.
func (s *server) GetAllLFSPointers(in *gitalypb.GetAllLFSPointersRequest, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	ctx := stream.Context()

	if err := validateGetAllLFSPointersRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetAllLFSPointers: %v", err)
	}

	repo := localrepo.New(s.gitCmdFactory, in.Repository, s.cfg)

	lfsPointers, err := findLFSPointersByRevisions(ctx, repo, s.gitCmdFactory, 0, "--all")
	if err != nil {
		if errors.Is(err, errInvalidRevision) {
			return status.Errorf(codes.InvalidArgument, err.Error())
		}
		return err
	}

	err = sliceLFSPointers(lfsPointers, func(slice []*gitalypb.LFSPointer) error {
		return stream.Send(&gitalypb.GetAllLFSPointersResponse{
			LfsPointers: slice,
		})
	})
	if err != nil {
		return err
	}

	return nil
}

func validateGetAllLFSPointersRequest(in *gitalypb.GetAllLFSPointersRequest) error {
	if in.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}
	return nil
}

// findLFSPointersByRevisions will return all LFS objects reachable via the given set of revisions.
// Revisions accept all syntax supported by git-rev-list(1).
func findLFSPointersByRevisions(
	ctx context.Context,
	repo *localrepo.Repo,
	gitCmdFactory git.CommandFactory,
	limit int,
	revisions ...string,
) (lfsPointers []*gitalypb.LFSPointer, returnErr error) {
	for _, revision := range revisions {
		if strings.HasPrefix(revision, "-") && revision != "--all" && revision != "--not" {
			return nil, fmt.Errorf("%w: %q", errInvalidRevision, revision)
		}
	}

	flags := []git.Option{
		git.Flag{Name: "--in-commit-order"},
		git.Flag{Name: "--objects"},
		git.Flag{Name: "--no-object-names"},
		git.Flag{Name: fmt.Sprintf("--filter=blob:limit=%d", lfsPointerMaxSize)},
	}
	if featureflag.IsEnabled(ctx, featureflag.LFSPointersUseBitmapIndex) {
		flags = append(flags, git.Flag{Name: "--use-bitmap-index"})
	}

	// git-rev-list(1) currently does not have any way to list all reachable objects of a
	// certain type.
	var revListStderr bytes.Buffer
	revlist, err := repo.Exec(ctx, git.SubCmd{
		Name:  "rev-list",
		Flags: flags,
		Args:  revisions,
	}, git.WithStderr(&revListStderr))
	if err != nil {
		return nil, fmt.Errorf("could not execute rev-list: %w", err)
	}
	defer func() {
		// There is no way to properly determine whether the process has exited because of
		// us signalling the context or because of any other means. We can only approximate
		// this by checking whether the process state is "signal: killed". Which again is
		// awful, but given that `Signaled()` status is also not accessible to us,
		// it's the best we could do.
		//
		// Let's not do any of this, it's awful. Instead, we can simply check whether a
		// limit was set and if the number of returned LFS pointers matches that limit. If
		// so, we found all LFS pointers which the user requested and needn't bother whether
		// git-rev-list(1) may have failed. So let's instead just have the RPCcontext cancel
		// the process.
		if limit > 0 && len(lfsPointers) == limit {
			return
		}

		if err := revlist.Wait(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("rev-list failed: %w, stderr: %q",
				err, revListStderr.String())
		}
	}()

	return readLFSPointers(ctx, repo, revlist, limit)
}

// readLFSPointers reads object IDs of potential LFS pointers from the given reader and for each of
// them, it will determine whether the referenced object is an LFS pointer. Objects which are not a
// valid LFS pointer will be ignored. Objects which do not exist result in an error.
func readLFSPointers(
	ctx context.Context,
	repo *localrepo.Repo,
	objectIDReader io.Reader,
	limit int,
) ([]*gitalypb.LFSPointer, error) {
	catfileBatch, err := repo.Exec(ctx, git.SubCmd{
		Name: "cat-file",
		Flags: []git.Option{
			git.Flag{Name: "--batch"},
			git.Flag{Name: "--buffer"},
		},
	}, git.WithStdin(objectIDReader))
	if err != nil {
		return nil, fmt.Errorf("could not execute cat-file: %w", err)
	}

	var lfsPointers []*gitalypb.LFSPointer
	reader := bufio.NewReader(catfileBatch)

	for {
		objectInfo, err := catfile.ParseObjectInfo(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("could not get LFS pointer info: %w", err)
		}

		data, err := ioutil.ReadAll(io.LimitReader(reader, objectInfo.Size+1))
		if err != nil {
			return nil, fmt.Errorf("could not read LFS pointer candidate: %w", err)
		}
		data = data[:len(data)-1]

		if objectInfo.Type != "blob" || !isLFSPointer(data) {
			continue
		}

		lfsPointers = append(lfsPointers, &gitalypb.LFSPointer{
			Data: data,
			Size: int64(len(data)),
			Oid:  objectInfo.Oid.String(),
		})

		// Exit early in case we've got all LFS pointers. We want to do this here instead of
		// just terminating the loop because we need to check git-cat-file(1)'s exit code in
		// case the loop finishes successfully via an EOF. We don't want to do so here
		// though: we don't care for successful termination of the command, we only care
		// that we've got all pointers. The command is then getting cancelled via the
		// parent's context.
		if limit > 0 && len(lfsPointers) >= limit {
			return lfsPointers, nil
		}
	}

	if err := catfileBatch.Wait(); err != nil {
		return nil, err
	}

	return lfsPointers, nil
}

// isLFSPointer determines whether the given blob contents are an LFS pointer or not.
func isLFSPointer(data []byte) bool {
	// TODO: this is incomplete as it does not recognize pre-release version of LFS blobs with
	// the "https://hawser.github.com/spec/v1" version. For compatibility with the Ruby RPC, we
	// leave this as-is for now though.
	return bytes.HasPrefix(data, []byte("version https://git-lfs.github.com/spec"))
}

// sliceLFSPointers slices the given pointers into subsets of slices with at most
// lfsPointerSliceSize many pointers and executes the given fallback function. If the callback
// returns an error, slicing is aborted and the error is returned verbosely.
func sliceLFSPointers(pointers []*gitalypb.LFSPointer, fn func([]*gitalypb.LFSPointer) error) error {
	chunkSize := lfsPointerSliceSize

	for {
		if len(pointers) == 0 {
			return nil
		}

		if len(pointers) < chunkSize {
			chunkSize = len(pointers)
		}

		if err := fn(pointers[:chunkSize]); err != nil {
			return err
		}

		pointers = pointers[chunkSize:]
	}
}
