package namespace

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	config.Config.Storages = nil

	for _, st := range []string{"default", "other"} {
		dir, err := filepath.Abs(filepath.Join("testdata", st))
		if err != nil {
			log.Fatal(err)
		}

		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}

		config.Config.Storages = append(config.Config.Storages,
			config.Storage{Name: st, Path: dir},
		)
	}

	return m.Run()
}

func TestNamespaceExists(t *testing.T) {
	server, serverSocketPath := runNamespaceServer(t)
	defer server.Stop()

	client, conn := newNamespaceClient(t, serverSocketPath)
	defer conn.Close()

	// Create one namespace for testing it exists
	ctx, cancel := testhelper.Context()
	defer cancel()

	const (
		existingStorage   = "default"
		existingNamespace = "existing"
	)

	storageDir := prepareStorageDir(t, existingStorage)
	require.NoError(t, os.MkdirAll(filepath.Join(storageDir, existingNamespace), 0755))

	queries := []struct {
		desc      string
		request   *gitalypb.NamespaceExistsRequest
		errorCode codes.Code
		exists    bool
	}{
		{
			desc: "empty name",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: existingStorage,
				Name:        "",
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "Namespace doesn't exists",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: existingStorage,
				Name:        "not-existing",
			},
			errorCode: codes.OK,
			exists:    false,
		},
		{
			desc: "Wrong storage path",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: "other",
				Name:        existingNamespace,
			},
			errorCode: codes.OK,
			exists:    false,
		},
		{
			desc: "Namespace exists",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: existingStorage,
				Name:        existingNamespace,
			},
			errorCode: codes.OK,
			exists:    true,
		},
	}

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			response, err := client.NamespaceExists(ctx, tc.request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			if tc.errorCode == codes.OK {
				require.Equal(t, tc.exists, response.Exists)
			}
		})
	}
}

func prepareStorageDir(t *testing.T, storageName string) string {
	storageDir, err := helper.GetStorageByName(storageName)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(storageDir))
	require.NoError(t, os.MkdirAll(storageDir, 0755))
	return storageDir
}

func TestAddNamespace(t *testing.T) {
	server, serverSocketPath := runNamespaceServer(t)
	defer server.Stop()

	client, conn := newNamespaceClient(t, serverSocketPath)
	defer conn.Close()

	const existingStorage = "default"
	storageDir := prepareStorageDir(t, existingStorage)

	queries := []struct {
		desc      string
		request   *gitalypb.AddNamespaceRequest
		errorCode codes.Code
	}{
		{
			desc: "No name",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: existingStorage,
				Name:        "",
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "Namespace is successfully created",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: existingStorage,
				Name:        "create-me",
			},
			errorCode: codes.OK,
		},
		{
			desc: "Idempotent on creation",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: existingStorage,
				Name:        "create-me",
			},
			errorCode: codes.OK,
		},
		{
			desc: "no storage",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: "",
				Name:        "mepmep",
			},
			errorCode: codes.InvalidArgument,
		},
	}

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.AddNamespace(ctx, tc.request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			// Clean up
			if tc.errorCode == codes.OK {
				require.Equal(t, existingStorage, tc.request.StorageName, "sanity check")

				requireIsDir(t, filepath.Join(storageDir, tc.request.Name))
			}
		})
	}
}

func TestRemoveNamespace(t *testing.T) {
	server, serverSocketPath := runNamespaceServer(t)
	defer server.Stop()

	client, conn := newNamespaceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	const (
		existingStorage   = "default"
		existingNamespace = "created"
	)

	storageDir := prepareStorageDir(t, existingStorage)

	queries := []struct {
		desc      string
		request   *gitalypb.RemoveNamespaceRequest
		errorCode codes.Code
	}{
		{
			desc: "Namespace is successfully removed",
			request: &gitalypb.RemoveNamespaceRequest{
				StorageName: existingStorage,
				Name:        existingNamespace,
			},
			errorCode: codes.OK,
		},
		{
			desc: "Idempotent on deletion",
			request: &gitalypb.RemoveNamespaceRequest{
				StorageName: existingStorage,
				Name:        "not-there",
			},
			errorCode: codes.OK,
		},
		{
			desc: "no storage",
			request: &gitalypb.RemoveNamespaceRequest{
				StorageName: "",
				Name:        "mepmep",
			},
			errorCode: codes.InvalidArgument,
		},
	}

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			require.NoError(t, os.MkdirAll(filepath.Join(storageDir, existingNamespace), 0755), "test setup")

			_, err := client.RemoveNamespace(ctx, tc.request)
			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			if tc.errorCode == codes.OK {
				require.Equal(t, existingStorage, tc.request.StorageName, "sanity check")
				testhelper.AssertPathNotExists(t, filepath.Join(storageDir, tc.request.Name))
			}
		})
	}
}

func TestRenameNamespace(t *testing.T) {
	server, serverSocketPath := runNamespaceServer(t)
	defer server.Stop()

	client, conn := newNamespaceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	const (
		existingStorage   = "default"
		existingNamespace = "existing"
	)

	storageDir := prepareStorageDir(t, existingStorage)
	require.NoError(t, os.MkdirAll(filepath.Join(storageDir, existingNamespace), 0755))

	queries := []struct {
		desc      string
		request   *gitalypb.RenameNamespaceRequest
		errorCode codes.Code
	}{
		{
			desc: "Renaming an existing namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        existingNamespace,
				To:          "new-path",
				StorageName: existingStorage,
			},
			errorCode: codes.OK,
		},
		{
			desc: "No from given",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "",
				To:          "new-path",
				StorageName: existingStorage,
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "non-existing namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "non-existing",
				To:          "new-path",
				StorageName: existingStorage,
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "existing destination namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        existingNamespace,
				To:          existingNamespace,
				StorageName: existingStorage,
			},
			errorCode: codes.InvalidArgument,
		},
	}

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.RenameNamespace(ctx, tc.request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			if tc.errorCode == codes.OK {
				toDir := filepath.Join(storageDir, tc.request.To)
				requireIsDir(t, toDir)
				require.NoError(t, os.RemoveAll(toDir))
			}
		})
	}
}

func requireIsDir(t *testing.T, dir string) {
	fi, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, fi.IsDir(), "%v is directory", dir)
}

func TestRenameNamespaceWithNonexistentParentDir(t *testing.T) {
	server, serverSocketPath := runNamespaceServer(t)
	defer server.Stop()

	client, conn := newNamespaceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.AddNamespace(ctx, &gitalypb.AddNamespaceRequest{
		StorageName: "default",
		Name:        "existing",
	})
	require.NoError(t, err)

	testCases := []struct {
		desc      string
		request   *gitalypb.RenameNamespaceRequest
		errorCode codes.Code
	}{
		{
			desc: "existing source, non existing target directory",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "existing",
				To:          "some/other/new-path",
				StorageName: "default",
			},
			errorCode: codes.OK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err = client.RenameNamespace(ctx, &gitalypb.RenameNamespaceRequest{
				From:        "existing",
				To:          "some/other/new-path",
				StorageName: "default"})
			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			if tc.errorCode == codes.OK {
				storagePath, err := helper.GetStorageByName(tc.request.StorageName)
				require.NoError(t, err)

				toDir := namespacePath(storagePath, tc.request.GetTo())

				requireIsDir(t, toDir)
				require.NoError(t, os.RemoveAll(toDir))
			}
		})
	}
}
