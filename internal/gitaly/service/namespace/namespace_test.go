package namespace

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cleanup := testhelper.Configure()
	defer cleanup()

	return m.Run()
}

func TestNamespaceExists(t *testing.T) {
	cfg, client := setupNamespaceService(t)
	existingStorage := cfg.Storages[0]

	ctx, cancel := testhelper.Context()
	defer cancel()

	const existingNamespace = "existing"
	require.NoError(t, os.MkdirAll(filepath.Join(existingStorage.Path, existingNamespace), 0755))

	queries := []struct {
		desc      string
		request   *gitalypb.NamespaceExistsRequest
		errorCode codes.Code
		exists    bool
	}{
		{
			desc: "empty name",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: existingStorage.Name,
				Name:        "",
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "Namespace doesn't exists",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: existingStorage.Name,
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
				StorageName: existingStorage.Name,
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

func getStorageDir(t *testing.T, cfg config.Cfg, storageName string) string {
	t.Helper()
	s, found := cfg.Storage(storageName)
	require.True(t, found)
	return s.Path
}

func TestAddNamespace(t *testing.T) {
	cfg, client := setupNamespaceService(t)
	existingStorage := cfg.Storages[0]

	queries := []struct {
		desc      string
		request   *gitalypb.AddNamespaceRequest
		errorCode codes.Code
	}{
		{
			desc: "No name",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: existingStorage.Name,
				Name:        "",
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "Namespace is successfully created",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: existingStorage.Name,
				Name:        "create-me",
			},
			errorCode: codes.OK,
		},
		{
			desc: "Idempotent on creation",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: existingStorage.Name,
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
				require.Equal(t, existingStorage.Name, tc.request.StorageName, "sanity check")

				require.DirExists(t, filepath.Join(existingStorage.Path, tc.request.Name))
			}
		})
	}
}

func TestRemoveNamespace(t *testing.T) {
	cfg, client := setupNamespaceService(t)
	existingStorage := cfg.Storages[0]

	ctx, cancel := testhelper.Context()
	defer cancel()

	const existingNamespace = "created"
	require.NoError(t, os.MkdirAll(filepath.Join(existingStorage.Path, existingNamespace), 0755), "test setup")

	queries := []struct {
		desc      string
		request   *gitalypb.RemoveNamespaceRequest
		errorCode codes.Code
	}{
		{
			desc: "Namespace is successfully removed",
			request: &gitalypb.RemoveNamespaceRequest{
				StorageName: existingStorage.Name,
				Name:        existingNamespace,
			},
			errorCode: codes.OK,
		},
		{
			desc: "Idempotent on deletion",
			request: &gitalypb.RemoveNamespaceRequest{
				StorageName: existingStorage.Name,
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
			_, err := client.RemoveNamespace(ctx, tc.request)
			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			if tc.errorCode == codes.OK {
				require.Equal(t, existingStorage.Name, tc.request.StorageName, "sanity check")
				testhelper.AssertPathNotExists(t, filepath.Join(existingStorage.Path, tc.request.Name))
			}
		})
	}
}

func TestRenameNamespace(t *testing.T) {
	cfg, client := setupNamespaceService(t)
	existingStorage := cfg.Storages[0]

	ctx, cancel := testhelper.Context()
	defer cancel()

	const existingNamespace = "existing"
	require.NoError(t, os.MkdirAll(filepath.Join(existingStorage.Path, existingNamespace), 0755))

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
				StorageName: existingStorage.Name,
			},
			errorCode: codes.OK,
		},
		{
			desc: "No from given",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "",
				To:          "new-path",
				StorageName: existingStorage.Name,
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "non-existing namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "non-existing",
				To:          "new-path",
				StorageName: existingStorage.Name,
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "existing destination namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        existingNamespace,
				To:          existingNamespace,
				StorageName: existingStorage.Name,
			},
			errorCode: codes.InvalidArgument,
		},
	}

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.RenameNamespace(ctx, tc.request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			if tc.errorCode == codes.OK {
				toDir := filepath.Join(existingStorage.Path, tc.request.To)
				require.DirExists(t, toDir)
				require.NoError(t, os.RemoveAll(toDir))
			}
		})
	}
}

func TestRenameNamespaceWithNonexistentParentDir(t *testing.T) {
	cfg, client := setupNamespaceService(t)
	existingStorage := cfg.Storages[0]

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.AddNamespace(ctx, &gitalypb.AddNamespaceRequest{
		StorageName: existingStorage.Name,
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
				StorageName: existingStorage.Name,
			},
			errorCode: codes.OK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err = client.RenameNamespace(ctx, &gitalypb.RenameNamespaceRequest{
				From:        "existing",
				To:          "some/other/new-path",
				StorageName: existingStorage.Name,
			})
			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			if tc.errorCode == codes.OK {
				storagePath := getStorageDir(t, cfg, tc.request.StorageName)
				require.NoError(t, err)

				toDir := namespacePath(storagePath, tc.request.GetTo())

				require.DirExists(t, toDir)
				require.NoError(t, os.RemoveAll(toDir))
			}
		})
	}
}
