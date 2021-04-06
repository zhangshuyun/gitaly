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

	if err := os.MkdirAll(backupPath, os.ModePerm); err != nil {
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

func (fs *Filesystem) newRepoClient(ctx context.Context, server storage.ServerInfo) (gitalypb.RepositoryServiceClient, error) {
	conn, err := fs.conns.Dial(ctx, server.Address, server.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRepositoryServiceClient(conn), nil
}

func writeFile(path string, r io.Reader) (returnErr error) {
	f, err := os.Create(path)
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
