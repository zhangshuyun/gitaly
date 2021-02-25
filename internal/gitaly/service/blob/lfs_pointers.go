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
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

// GetLFSPointers takes the list of requested blob IDs and filters them down to blobs which are
// valid LFS pointers. It is fine to pass blob IDs which do not point to a valid LFS pointer, but
// passing blob IDs which do not exist results in an error.
func (s *server) GetLFSPointers(req *gitalypb.GetLFSPointersRequest, stream gitalypb.BlobService_GetLFSPointersServer) error {
	ctx := stream.Context()

	if err := validateGetLFSPointersRequest(req); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetLFSPointers: %v", err)
	}

	if featureflag.IsDisabled(ctx, featureflag.GoGetLFSPointers) {
		return s.rubyGetLFSPointers(req, stream)
	}

	repo := localrepo.New(s.gitCmdFactory, req.Repository, s.cfg)
	objectIDs := strings.Join(req.BlobIds, "\n")

	lfsPointers, err := readLFSPointers(ctx, repo, s.gitCmdFactory, strings.NewReader(objectIDs), false, 0)
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

func (s *server) rubyGetLFSPointers(req *gitalypb.GetLFSPointersRequest, stream gitalypb.BlobService_GetLFSPointersServer) error {
	ctx := stream.Context()

	client, err := s.ruby.BlobServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, req.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetLFSPointers(clientCtx, req)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
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

	if featureflag.IsDisabled(ctx, featureflag.GoGetNewLFSPointers) {
		return s.rubyGetNewLFSPointers(in, stream)
	}

	repo := localrepo.New(s.gitCmdFactory, in.Repository, s.cfg)

	var refs []string
	var opts []git.Option

	if in.NotInAll {
		refs = []string{string(in.Revision)}
		// We need to append another `--not` to cancel out the first one. Otherwise, we'd
		// negate the user-provided revision.
		opts = []git.Option{
			git.Flag{"--not"}, git.Flag{"--all"}, git.Flag{"--not"},
		}
	} else {
		refs = make([]string, len(in.NotInRefs)+1)
		refs[0] = string(in.Revision)

		// We cannot intermix references and flags because of safety guards of our git DSL.
		// Instead, we thus manually negate all references by prefixing them with the caret
		// symbol.
		for i, notInRef := range in.NotInRefs {
			refs[i+1] = "^" + string(notInRef)
		}
	}

	lfsPointers, err := findLFSPointersByRevisions(ctx, repo, s.gitCmdFactory, opts, int(in.Limit), refs...)
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

func (s *server) rubyGetNewLFSPointers(in *gitalypb.GetNewLFSPointersRequest, stream gitalypb.BlobService_GetNewLFSPointersServer) error {
	ctx := stream.Context()

	client, err := s.ruby.BlobServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, in.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetNewLFSPointers(clientCtx, in)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
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

	if featureflag.IsDisabled(ctx, featureflag.GoGetAllLFSPointers) {
		return s.rubyGetAllLFSPointers(in, stream)
	}

	repo := localrepo.New(s.gitCmdFactory, in.Repository, s.cfg)

	lfsPointers, err := findLFSPointersByRevisions(ctx, repo, s.gitCmdFactory, []git.Option{
		git.Flag{Name: "--all"},
	}, 0)
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

func (s *server) rubyGetAllLFSPointers(in *gitalypb.GetAllLFSPointersRequest, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	ctx := stream.Context()

	client, err := s.ruby.BlobServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, in.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetAllLFSPointers(clientCtx, in)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
}

func validateGetAllLFSPointersRequest(in *gitalypb.GetAllLFSPointersRequest) error {
	if in.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}
	return nil
}

// findLFSPointersByRevisions will return all LFS objects reachable via the given set of revisions.
// Revisions accept all syntax supported by git-rev-list(1). This function also accepts a set of
// options accepted by git-rev-list(1). Note that because git.Commands do not accept dashed
// positional arguments, it is currently not possible to mix options and revisions (e.g. "git
// rev-list master --not feature").
func findLFSPointersByRevisions(
	ctx context.Context,
	repo *localrepo.Repo,
	gitCmdFactory git.CommandFactory,
	opts []git.Option,
	limit int,
	revisions ...string,
) (_ []*gitalypb.LFSPointer, returnErr error) {
	for _, revision := range revisions {
		if strings.HasPrefix(revision, "-") && revision != "--all" && revision != "--not" {
			return nil, fmt.Errorf("%w: %q", errInvalidRevision, revision)
		}
	}

	// git-rev-list(1) currently does not have any way to list all reachable objects of a
	// certain type. As an optimization, we thus call it with `--object-names`, which will
	// print an associated name for a given revision, if there is any. This allows us to skip
	// at least some objects, namely all commits.
	//
	// It is questionable whether this optimization helps at all: by including object names,
	// git cannot make use of bitmap indices. We thus pessimize git-rev-list(1) and optimize
	// git-cat-file(1). And given that there's going to be a lot more blobs and trees (which
	// both _do_ have an object name) than commits, it's probably an optimization which even
	// slows down execution. Still, we keep this to stay compatible with the Ruby
	// implementation.
	var revListStderr bytes.Buffer
	revlist, err := repo.Exec(ctx, nil, git.SubCmd{
		Name: "rev-list",
		Flags: append([]git.Option{
			git.Flag{Name: "--in-commit-order"},
			git.Flag{Name: "--objects"},
			git.Flag{Name: "--object-names"},
			git.Flag{Name: fmt.Sprintf("--filter=blob:limit=%d", lfsPointerMaxSize)},
		}, opts...),
		Args: revisions,
	}, git.WithStderr(&revListStderr))
	if err != nil {
		return nil, fmt.Errorf("could not execute rev-list: %w", err)
	}
	defer func() {
		if err := revlist.Wait(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("rev-list failed: %w, stderr: %q",
				err, revListStderr.String())
		}
	}()

	return readLFSPointers(ctx, repo, gitCmdFactory, revlist, true, limit)
}

// readLFSPointers reads object IDs of potential LFS pointers from the given reader and for each of
// them, it will determine whether the referenced object is an LFS pointer. Objects which are not a
// valid LFS pointer will be ignored. Objects which do not exist result in an error.
//
// If filterByObjectName is set to true, only IDs which have an associated object name will be
// read. This is helpful to pass output of git-rev-list(1) with `--object-names` directly to this
// function.
func readLFSPointers(
	ctx context.Context,
	repo *localrepo.Repo,
	gitCmdFactory git.CommandFactory,
	objectIDReader io.Reader,
	filterByObjectName bool,
	limit int,
) ([]*gitalypb.LFSPointer, error) {
	catfileBatch, err := catfile.New(ctx, gitCmdFactory, repo)
	if err != nil {
		return nil, fmt.Errorf("could not execute cat-file: %w", err)
	}

	var lfsPointers []*gitalypb.LFSPointer
	scanner := bufio.NewScanner(objectIDReader)

	for scanner.Scan() {
		revision := git.Revision(scanner.Bytes())
		if filterByObjectName {
			revAndPath := strings.SplitN(revision.String(), " ", 2)
			if len(revAndPath) != 2 {
				continue
			}
			revision = git.Revision(revAndPath[0])
		}

		objectInfo, err := catfileBatch.Info(ctx, revision)
		if err != nil {
			return nil, fmt.Errorf("could not get LFS pointer info: %w", err)
		}

		if objectInfo.Type != "blob" {
			continue
		}

		blob, err := catfileBatch.Blob(ctx, revision)
		if err != nil {
			return nil, fmt.Errorf("could not retrieve LFS pointer: %w", err)
		}

		data, err := ioutil.ReadAll(blob.Reader)
		if err != nil {
			return nil, fmt.Errorf("could not read LFS pointer: %w", err)
		}

		if !isLFSPointer(data) {
			continue
		}

		lfsPointers = append(lfsPointers, &gitalypb.LFSPointer{
			Data: data,
			Size: int64(len(data)),
			Oid:  revision.String(),
		})

		if limit > 0 && len(lfsPointers) >= limit {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning LFS pointers failed: %w", err)
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
