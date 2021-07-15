package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	bundlePath := backupPath + ".bundle"
	customHooksPath := filepath.Join(backupPath, "custom_hooks.tar")

	if err := mgr.writeBundle(ctx, bundlePath, req.Server, req.Repository); err != nil {
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

func (mgr *Manager) writeBundle(ctx context.Context, path string, server storage.ServerInfo, repo *gitalypb.Repository) error {
	repoClient, err := mgr.newRepoClient(ctx, server)
	if err != nil {
		return err
	}
	stream, err := repoClient.CreateBundle(ctx, &gitalypb.CreateBundleRequest{Repository: repo})
	if err != nil {
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

func (mgr *Manager) newRepoClient(ctx context.Context, server storage.ServerInfo) (gitalypb.RepositoryServiceClient, error) {
	conn, err := mgr.conns.Dial(ctx, server.Address, server.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRepositoryServiceClient(conn), nil
}
