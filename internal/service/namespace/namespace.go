package namespace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

var noNameError = helper.ErrInvalidArgumentf("name: cannot be empty")

func (s *server) NamespaceExists(ctx context.Context, in *gitalypb.NamespaceExistsRequest) (*gitalypb.NamespaceExistsResponse, error) {
	storagePath, err := helper.GetStorageByName(in.GetStorageName())
	if err != nil {
		return nil, err
	}

	// This case should return an error, as else we'd actually say the path
	// exists as the storage exists
	if in.GetName() == "" {
		return nil, noNameError
	}

	exists, err := directoryExists(storagePath, in.GetName())

	return &gitalypb.NamespaceExistsResponse{Exists: exists}, helper.ErrInternal(err)
}

func (s *server) AddNamespace(ctx context.Context, in *gitalypb.AddNamespaceRequest) (*gitalypb.AddNamespaceResponse, error) {
	storagePath, err := helper.GetStorageByName(in.GetStorageName())
	if err != nil {
		return nil, err
	}

	name := in.GetName()
	if name == "" {
		return nil, noNameError
	}

	if err := createDirectory(storagePath, name); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.AddNamespaceResponse{}, nil
}

func (s *server) RenameNamespace(ctx context.Context, in *gitalypb.RenameNamespaceRequest) (*gitalypb.RenameNamespaceResponse, error) {
	if err := validateRenameNamespaceRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	storagePath, err := helper.GetStorageByName(in.GetStorageName())
	if err != nil {
		return nil, err
	}

	fromPath, toPath := in.GetFrom(), in.GetTo()

	if err = createDirectory(storagePath, filepath.Dir(toPath)); err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err = os.Rename(namespacePath(storagePath, fromPath), namespacePath(storagePath, toPath)); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.RenameNamespaceResponse{}, nil
}

func (s *server) RemoveNamespace(ctx context.Context, in *gitalypb.RemoveNamespaceRequest) (*gitalypb.RemoveNamespaceResponse, error) {
	storagePath, err := helper.GetStorageByName(in.GetStorageName())
	if err != nil {
		return nil, err
	}

	// Needed as else we might destroy the whole storage
	name := in.GetName()
	if name == "" {
		return nil, noNameError
	}

	// os.RemoveAll is idempotent by itself
	// No need to check if the directory exists, or not
	if err = os.RemoveAll(namespacePath(storagePath, name)); err != nil {
		return nil, helper.ErrInternal(err)
	}
	return &gitalypb.RemoveNamespaceResponse{}, nil
}

func namespacePath(storagePath, ns string) string {
	return filepath.Join(storagePath, ns)
}

func createDirectory(storagePath, namespace string) error {
	return os.MkdirAll(namespacePath(storagePath, namespace), 0755)
}

func directoryExists(storagePath, namespace string) (bool, error) {
	fi, err := os.Stat(namespacePath(storagePath, namespace))
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if !fi.IsDir() {
		return false, fmt.Errorf("expected directory, found file %s", namespace)
	}

	return true, nil
}

func validateRenameNamespaceRequest(req *gitalypb.RenameNamespaceRequest) error {
	if _, err := helper.GetStorageByName(req.GetStorageName()); err != nil {
		return err
	}

	if req.GetFrom() == "" {
		return errors.New("from field cannot be empty")
	}
	if req.GetTo() == "" {
		return errors.New("to field cannot be empty")
	}

	return nil
}
