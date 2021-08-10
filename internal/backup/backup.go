package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"gocloud.dev/blob/azureblob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrSkipped means the repository was skipped because there was nothing to backup
	ErrSkipped = errors.New("repository skipped")
	// ErrDoesntExist means that the data was not found.
	ErrDoesntExist = errors.New("doesn't exist")
)

// Sink is an abstraction over the real storage used for storing/restoring backups.
type Sink interface {
	// Write saves all the data from the r by relativePath.
	Write(ctx context.Context, relativePath string, r io.Reader) error
	// GetReader returns a reader that servers the data stored by relativePath.
	// If relativePath doesn't exists the ErrDoesntExist will be returned.
	GetReader(ctx context.Context, relativePath string) (io.ReadCloser, error)
}

// ResolveSink returns a sink implementation based on the provided path.
func ResolveSink(ctx context.Context, path string) (Sink, error) {
	parsed, err := url.Parse(path)
	if err != nil {
		return nil, err
	}
	scheme := parsed.Scheme
	if i := strings.LastIndex(scheme, "+"); i > 0 {
		// the url may include additional configuration options like service name
		// we don't include it into the scheme definition as it will push us to create
		// a full set of variations. Instead we trim it up to the service option only.
		scheme = scheme[i+1:]
	}

	switch scheme {
	case s3blob.Scheme, azureblob.Scheme, gcsblob.Scheme:
		sink, err := NewStorageServiceSink(ctx, path)
		return sink, err
	default:
		return NewFilesystemSink(path), nil
	}
}

// Manager manages process of the creating/restoring backups.
type Manager struct {
	sink  Sink
	conns *client.Pool
}

// NewManager creates and returns initialized *Manager instance.
func NewManager(sink Sink) *Manager {
	return &Manager{
		sink:  sink,
		conns: client.NewPool(),
	}
}

// CreateRequest is the request to create a backup
type CreateRequest struct {
	Server     storage.ServerInfo
	Repository *gitalypb.Repository
}

// Create creates a repository backup.
func (mgr *Manager) Create(ctx context.Context, req *CreateRequest) error {
	if isEmpty, err := mgr.isEmpty(ctx, req.Server, req.Repository); err != nil {
		return fmt.Errorf("manager: %w", err)
	} else if isEmpty {
		return ErrSkipped
	}

	backupPath := strings.TrimSuffix(req.Repository.RelativePath, ".git")
	refsPath := backupPath + ".refs"
	bundlePath := backupPath + ".bundle"
	customHooksPath := filepath.Join(backupPath, "custom_hooks.tar")

	refs, err := mgr.listRefs(ctx, req.Server, req.Repository)
	if err != nil {
		return fmt.Errorf("manager: %w", err)
	}
	if err := mgr.writeRefs(ctx, refsPath, refs); err != nil {
		return fmt.Errorf("manager: %w", err)
	}
	patterns := mgr.generatePatterns(refs)
	if err := mgr.writeBundle(ctx, bundlePath, req.Server, req.Repository, patterns); err != nil {
		return fmt.Errorf("manager: write bundle: %w", err)
	}
	if err := mgr.writeCustomHooks(ctx, customHooksPath, req.Server, req.Repository); err != nil {
		return fmt.Errorf("manager: write custom hooks: %w", err)
	}

	return nil
}

// RestoreRequest is the request to restore from a backup
type RestoreRequest struct {
	Server       storage.ServerInfo
	Repository   *gitalypb.Repository
	AlwaysCreate bool
}

// Restore restores a repository from a backup.
func (mgr *Manager) Restore(ctx context.Context, req *RestoreRequest) error {
	backupPath := strings.TrimSuffix(req.Repository.RelativePath, ".git")
	bundlePath := backupPath + ".bundle"
	customHooksPath := filepath.Join(backupPath, "custom_hooks.tar")

	if err := mgr.removeRepository(ctx, req.Server, req.Repository); err != nil {
		return fmt.Errorf("manager: %w", err)
	}
	if err := mgr.restoreBundle(ctx, bundlePath, req.Server, req.Repository); err != nil {
		// For compatibility with existing backups we need to always create the
		// repository even if there's no bundle for project repositories
		// (not wiki or snippet repositories).  Gitaly does not know which
		// repository is which type so here we accept a parameter to tell us
		// to employ this behaviour.
		if req.AlwaysCreate && errors.Is(err, ErrSkipped) {
			if err := mgr.createRepository(ctx, req.Server, req.Repository); err != nil {
				return fmt.Errorf("manager: %w", err)
			}
		} else {
			return fmt.Errorf("manager: %w", err)
		}
	}
	if err := mgr.restoreCustomHooks(ctx, customHooksPath, req.Server, req.Repository); err != nil {
		return fmt.Errorf("manager: %w", err)
	}
	return nil
}

func (mgr *Manager) isEmpty(ctx context.Context, server storage.ServerInfo, repo *gitalypb.Repository) (bool, error) {
	repoClient, err := mgr.newRepoClient(ctx, server)
	if err != nil {
		return false, fmt.Errorf("isEmpty: %w", err)
	}
	hasLocalBranches, err := repoClient.HasLocalBranches(ctx, &gitalypb.HasLocalBranchesRequest{Repository: repo})
	switch {
	case status.Code(err) == codes.NotFound:
		return true, nil
	case err != nil:
		return false, fmt.Errorf("isEmpty: %w", err)
	}
	return !hasLocalBranches.GetValue(), nil
}

func (mgr *Manager) removeRepository(ctx context.Context, server storage.ServerInfo, repo *gitalypb.Repository) error {
	repoClient, err := mgr.newRepoClient(ctx, server)
	if err != nil {
		return fmt.Errorf("remove repository: %w", err)
	}
	if _, err := repoClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo}); err != nil {
		return fmt.Errorf("remove repository: %w", err)
	}
	return nil
}

func (mgr *Manager) createRepository(ctx context.Context, server storage.ServerInfo, repo *gitalypb.Repository) error {
	repoClient, err := mgr.newRepoClient(ctx, server)
	if err != nil {
		return fmt.Errorf("create repository: %w", err)
	}
	if _, err := repoClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repo}); err != nil {
		return fmt.Errorf("create repository: %w", err)
	}
	return nil
}

func (mgr *Manager) writeBundle(ctx context.Context, path string, server storage.ServerInfo, repo *gitalypb.Repository, patterns [][]byte) error {
	repoClient, err := mgr.newRepoClient(ctx, server)
	if err != nil {
		return err
	}
	stream, err := repoClient.CreateBundleFromRefList(ctx)
	if err != nil {
		return err
	}
	c := chunk.New(&createBundleFromRefListSender{
		stream: stream,
	})
	for _, pattern := range patterns {
		if err := c.Send(&gitalypb.CreateBundleFromRefListRequest{
			Repository: repo,
			Patterns:   [][]byte{pattern},
		}); err != nil {
			return err
		}
	}
	if err := c.Flush(); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	bundle := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})

	if err := mgr.sink.Write(ctx, path, bundle); err != nil {
		return fmt.Errorf("%T write: %w", mgr.sink, err)
	}
	return nil
}

type createBundleFromRefListSender struct {
	stream gitalypb.RepositoryService_CreateBundleFromRefListClient
	chunk  gitalypb.CreateBundleFromRefListRequest
}

// Reset should create a fresh response message.
func (s *createBundleFromRefListSender) Reset() {
	s.chunk = gitalypb.CreateBundleFromRefListRequest{}
}

// Append should append the given item to the slice in the current response message
func (s *createBundleFromRefListSender) Append(msg proto.Message) {
	req := msg.(*gitalypb.CreateBundleFromRefListRequest)
	s.chunk.Repository = req.GetRepository()
	s.chunk.Patterns = append(s.chunk.Patterns, req.Patterns...)
}

// Send should send the current response message
func (s *createBundleFromRefListSender) Send() error {
	return s.stream.Send(&s.chunk)
}

func (mgr *Manager) restoreBundle(ctx context.Context, path string, server storage.ServerInfo, repo *gitalypb.Repository) error {
	reader, err := mgr.sink.GetReader(ctx, path)
	if err != nil {
		if errors.Is(err, ErrDoesntExist) {
			return fmt.Errorf("%w: bundle does not exist: %q", ErrSkipped, path)
		}
		return fmt.Errorf("restore bundle: %w", err)
	}
	defer reader.Close()

	repoClient, err := mgr.newRepoClient(ctx, server)
	if err != nil {
		return fmt.Errorf("restore bundle: %q: %w", path, err)
	}
	stream, err := repoClient.CreateRepositoryFromBundle(ctx)
	if err != nil {
		return fmt.Errorf("restore bundle: %q: %w", path, err)
	}
	request := &gitalypb.CreateRepositoryFromBundleRequest{Repository: repo}
	bundle := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		// Only set `Repository` on the first `Send` of the stream
		request = &gitalypb.CreateRepositoryFromBundleRequest{}

		return nil
	})
	if _, err := io.Copy(bundle, reader); err != nil {
		return fmt.Errorf("restore bundle: %q: %w", path, err)
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("restore bundle: %q: %w", path, err)
	}
	return nil
}

func (mgr *Manager) writeCustomHooks(ctx context.Context, path string, server storage.ServerInfo, repo *gitalypb.Repository) error {
	repoClient, err := mgr.newRepoClient(ctx, server)
	if err != nil {
		return err
	}
	stream, err := repoClient.BackupCustomHooks(ctx, &gitalypb.BackupCustomHooksRequest{Repository: repo})
	if err != nil {
		return err
	}
	hooks := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	if err := mgr.sink.Write(ctx, path, hooks); err != nil {
		return fmt.Errorf("%T write: %w", mgr.sink, err)
	}
	return nil
}

func (mgr *Manager) restoreCustomHooks(ctx context.Context, path string, server storage.ServerInfo, repo *gitalypb.Repository) error {
	reader, err := mgr.sink.GetReader(ctx, path)
	if err != nil {
		if errors.Is(err, ErrDoesntExist) {
			return nil
		}
		return fmt.Errorf("restore custom hooks: %w", err)
	}
	defer reader.Close()

	repoClient, err := mgr.newRepoClient(ctx, server)
	if err != nil {
		return fmt.Errorf("restore custom hooks, %q: %w", path, err)
	}
	stream, err := repoClient.RestoreCustomHooks(ctx)
	if err != nil {
		return fmt.Errorf("restore custom hooks, %q: %w", path, err)
	}

	request := &gitalypb.RestoreCustomHooksRequest{Repository: repo}
	bundle := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		// Only set `Repository` on the first `Send` of the stream
		request = &gitalypb.RestoreCustomHooksRequest{}

		return nil
	})
	if _, err := io.Copy(bundle, reader); err != nil {
		return fmt.Errorf("restore custom hooks, %q: %w", path, err)
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("restore custom hooks, %q: %w", path, err)
	}
	return nil
}

// listRefs fetches the full set of refs and targets for the repository
func (mgr *Manager) listRefs(ctx context.Context, server storage.ServerInfo, repo *gitalypb.Repository) ([]*gitalypb.ListRefsResponse_Reference, error) {
	refClient, err := mgr.newRefClient(ctx, server)
	if err != nil {
		return nil, fmt.Errorf("list refs: %w", err)
	}
	stream, err := refClient.ListRefs(ctx, &gitalypb.ListRefsRequest{
		Repository: repo,
		Head:       true,
		Patterns:   [][]byte{[]byte("refs/")},
	})
	if err != nil {
		return nil, fmt.Errorf("list refs: %w", err)
	}

	var refs []*gitalypb.ListRefsResponse_Reference

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("list refs: %w", err)
		}
		refs = append(refs, resp.GetReferences()...)
	}

	return refs, nil
}

// writeRefs writes the previously fetched list of refs in the same output
// format as `git-show-ref(1)`
func (mgr *Manager) writeRefs(ctx context.Context, path string, refs []*gitalypb.ListRefsResponse_Reference) error {
	r, w := io.Pipe()
	go func() {
		var err error
		defer func() {
			_ = w.CloseWithError(err) // io.PipeWriter.Close* does not return an error
		}()
		for _, ref := range refs {
			_, err = fmt.Fprintf(w, "%s %s\n", ref.GetTarget(), ref.GetName())
			if err != nil {
				return
			}
		}
	}()

	err := mgr.sink.Write(ctx, path, r)
	if err != nil {
		return fmt.Errorf("write refs: %w", err)
	}

	return nil
}

func (mgr *Manager) generatePatterns(refs []*gitalypb.ListRefsResponse_Reference) [][]byte {
	var patterns [][]byte
	for _, ref := range refs {
		patterns = append(patterns, ref.GetName())
	}
	return patterns
}

func (mgr *Manager) newRepoClient(ctx context.Context, server storage.ServerInfo) (gitalypb.RepositoryServiceClient, error) {
	conn, err := mgr.conns.Dial(ctx, server.Address, server.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRepositoryServiceClient(conn), nil
}

func (mgr *Manager) newRefClient(ctx context.Context, server storage.ServerInfo) (gitalypb.RefServiceClient, error) {
	conn, err := mgr.conns.Dial(ctx, server.Address, server.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRefServiceClient(conn), nil
}
