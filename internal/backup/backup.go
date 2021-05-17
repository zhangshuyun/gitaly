package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrSkipped means the repository was skipped because there was nothing to backup
var ErrSkipped = errors.New("repository skipped")

// Filesystem strategy for creating and restoring backups
type Filesystem struct {
	path  string
	conns *client.Pool
}

// NewFilesystem creates a new Filesystem strategy
func NewFilesystem(path string) *Filesystem {
	return &Filesystem{
		path:  path,
		conns: client.NewPool(),
	}
}

// BackupRepository creates a repository backup on a local filesystem
func (fs *Filesystem) BackupRepository(ctx context.Context, server storage.ServerInfo, repo *gitalypb.Repository) error {
	if isEmpty, err := fs.isEmpty(ctx, server, repo); err != nil {
		return fmt.Errorf("backup: %w", err)
	} else if isEmpty {
		return ErrSkipped
	}

	backupPath := strings.TrimSuffix(filepath.Join(fs.path, repo.RelativePath), ".git")
	bundlePath := backupPath + ".bundle"
	customHooksPath := filepath.Join(backupPath, "custom_hooks.tar")

	if err := os.MkdirAll(backupPath, 0700); err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	if err := fs.writeBundle(ctx, bundlePath, server, repo); err != nil {
		return fmt.Errorf("backup: write bundle: %w", err)
	}
	if err := fs.writeCustomHooks(ctx, customHooksPath, server, repo); err != nil {
		return fmt.Errorf("backup: write custom hooks: %w", err)
	}

	return nil
}

// RestoreRepository restores a repository from a backup on a local filesystem
func (fs *Filesystem) RestoreRepository(ctx context.Context, server storage.ServerInfo, repo *gitalypb.Repository, alwaysCreate bool) error {
	backupPath := strings.TrimSuffix(filepath.Join(fs.path, repo.RelativePath), ".git")
	bundlePath := backupPath + ".bundle"
	customHooksPath := filepath.Join(backupPath, "custom_hooks.tar")

	if err := fs.removeRepository(ctx, server, repo); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	if err := fs.restoreBundle(ctx, bundlePath, server, repo); err != nil {
		// For compatibility with existing backups we need to always create the
		// repository even if there's no bundle for project repositories
		// (not wiki or snippet repositories).  Gitaly does not know which
		// repository is which type so here we accept a parameter to tell us
		// to employ this behaviour.
		if alwaysCreate && errors.Is(err, ErrSkipped) {
			if err := fs.createRepository(ctx, server, repo); err != nil {
				return fmt.Errorf("restore: %w", err)
			}
		} else {
			return fmt.Errorf("restore: %w", err)
		}
	}
	if err := fs.restoreCustomHooks(ctx, customHooksPath, server, repo); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	return nil
}

func (fs *Filesystem) isEmpty(ctx context.Context, server storage.ServerInfo, repo *gitalypb.Repository) (bool, error) {
	repoClient, err := fs.newRepoClient(ctx, server)
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

func (fs *Filesystem) removeRepository(ctx context.Context, server storage.ServerInfo, repo *gitalypb.Repository) error {
	repoClient, err := fs.newRepoClient(ctx, server)
	if err != nil {
		return fmt.Errorf("remove repository: %w", err)
	}
	if _, err := repoClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo}); err != nil {
		return fmt.Errorf("remove repository: %w", err)
	}
	return nil
}

func (fs *Filesystem) createRepository(ctx context.Context, server storage.ServerInfo, repo *gitalypb.Repository) error {
	repoClient, err := fs.newRepoClient(ctx, server)
	if err != nil {
		return fmt.Errorf("create repository: %w", err)
	}
	if _, err := repoClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repo}); err != nil {
		return fmt.Errorf("create repository: %w", err)
	}
	return nil
}

func (fs *Filesystem) writeBundle(ctx context.Context, path string, server storage.ServerInfo, repo *gitalypb.Repository) error {
	repoClient, err := fs.newRepoClient(ctx, server)
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
	return writeFile(path, bundle)
}

func (fs *Filesystem) restoreBundle(ctx context.Context, path string, server storage.ServerInfo, repo *gitalypb.Repository) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: bundle does not exist: %q", ErrSkipped, path)
		}
		return fmt.Errorf("restore bundle: %w", err)
	}
	defer f.Close()

	repoClient, err := fs.newRepoClient(ctx, server)
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
	if _, err := io.Copy(bundle, f); err != nil {
		return fmt.Errorf("restore bundle: %q: %w", path, err)
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("restore bundle: %q: %w", path, err)
	}
	return nil
}

func (fs *Filesystem) writeCustomHooks(ctx context.Context, path string, server storage.ServerInfo, repo *gitalypb.Repository) error {
	repoClient, err := fs.newRepoClient(ctx, server)
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
	if err := writeFile(path, hooks); err != nil {
		return err
	}
	return nil
}

func (fs *Filesystem) restoreCustomHooks(ctx context.Context, path string, server storage.ServerInfo, repo *gitalypb.Repository) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("restore custom hooks: %w", err)
	}
	defer f.Close()

	repoClient, err := fs.newRepoClient(ctx, server)
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
	if _, err := io.Copy(bundle, f); err != nil {
		return fmt.Errorf("restore custom hooks, %q: %w", path, err)
	}
	if _, err = stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("restore custom hooks, %q: %w", path, err)
	}
	return nil
}

func (fs *Filesystem) newRepoClient(ctx context.Context, server storage.ServerInfo) (gitalypb.RepositoryServiceClient, error) {
	conn, err := fs.conns.Dial(ctx, server.Address, server.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRepositoryServiceClient(conn), nil
}

func writeFile(path string, r io.Reader) (returnErr error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("write file: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("write file: %w", err)
		}
	}()
	size, err := io.Copy(f, r)
	if err != nil {
		return fmt.Errorf("write file %q: %w", path, err)
	}
	if size == 0 {
		// If the file is empty means that we received an empty stream, we delete the file
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("write file: %w", err)
		}
		return nil
	}
	return nil
}
