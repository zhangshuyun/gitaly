package namespace

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	storageOtherDir, _ := ioutil.TempDir("", "gitaly-repository-exists-test")
	defer os.Remove(storageOtherDir)

	oldStorages := config.Config.Storages
	config.ModifyStorages([]config.Storage{
		{Name: "default", Path: testhelper.GitlabTestStoragePath()},
		{Name: "other", Path: storageOtherDir},
	})
	defer func() { config.ModifyStorages(oldStorages) }()

	return m.Run()
}

func TestNamespaceExists(t *testing.T) {
	server, serverSocketPath := runNamespaceServer(t)
	defer server.Stop()

	client, conn := newNamespaceClient(t, serverSocketPath)
	defer conn.Close()

	// Create one namespace for testing it exists
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := client.AddNamespace(ctx, &gitalypb.AddNamespaceRequest{StorageName: "default", Name: "existing"})
	require.NoError(t, err, "Namespace creation failed")
	defer client.RemoveNamespace(ctx, &gitalypb.RemoveNamespaceRequest{StorageName: "default", Name: "existing"})

	queries := []struct {
		desc      string
		request   *gitalypb.NamespaceExistsRequest
		errorCode codes.Code
		exists    bool
	}{
		{
			desc: "empty name",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: "default",
				Name:        "",
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "Namespace doesn't exists",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: "default",
				Name:        "not-existing",
			},
			errorCode: codes.OK,
			exists:    false,
		},
		{
			desc: "Wrong storage path",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: "other",
				Name:        "existing",
			},
			errorCode: codes.OK,
			exists:    false,
		},
		{
			desc: "Namespace exists",
			request: &gitalypb.NamespaceExistsRequest{
				StorageName: "default",
				Name:        "existing",
			},
			errorCode: codes.OK,
			exists:    true,
		},
	}

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response, err := client.NamespaceExists(ctx, tc.request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			if tc.errorCode == codes.OK {
				require.Equal(t, tc.exists, response.Exists)
			}
		})
	}
}

func TestAddNamespace(t *testing.T) {
	server, serverSocketPath := runNamespaceServer(t)
	defer server.Stop()

	client, conn := newNamespaceClient(t, serverSocketPath)
	defer conn.Close()

	queries := []struct {
		desc      string
		request   *gitalypb.AddNamespaceRequest
		errorCode codes.Code
	}{
		{
			desc: "No name",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: "default",
				Name:        "",
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "Namespace is successfully created",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: "default",
				Name:        "create-me",
			},
			errorCode: codes.OK,
		},
		{
			desc: "Idempotent on creation",
			request: &gitalypb.AddNamespaceRequest{
				StorageName: "default",
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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			_, err := client.AddNamespace(ctx, tc.request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			// Clean up
			if tc.errorCode == codes.OK {
				client.RemoveNamespace(ctx, &gitalypb.RemoveNamespaceRequest{
					StorageName: tc.request.StorageName,
					Name:        tc.request.Name,
				})
			}
		})
	}
}

func TestRemoveNamespace(t *testing.T) {
	server, serverSocketPath := runNamespaceServer(t)
	defer server.Stop()

	client, conn := newNamespaceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := []struct {
		desc      string
		request   *gitalypb.RemoveNamespaceRequest
		errorCode codes.Code
	}{
		{
			desc: "Namespace is successfully removed",
			request: &gitalypb.RemoveNamespaceRequest{
				StorageName: "default",
				Name:        "created",
			},
			errorCode: codes.OK,
		},
		{
			desc: "Idempotent on deletion",
			request: &gitalypb.RemoveNamespaceRequest{
				StorageName: "default",
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
			_, err := client.AddNamespace(ctx, &gitalypb.AddNamespaceRequest{StorageName: "default", Name: "created"})
			require.NoError(t, err, "setup failed")

			_, err = client.RemoveNamespace(ctx, tc.request)
			require.Equal(t, tc.errorCode, helper.GrpcCode(err))
		})
	}
}

func TestRenameNamespace(t *testing.T) {
	server, serverSocketPath := runNamespaceServer(t)
	defer server.Stop()

	client, conn := newNamespaceClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	queries := []struct {
		desc      string
		request   *gitalypb.RenameNamespaceRequest
		errorCode codes.Code
	}{
		{
			desc: "Renaming an existing namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "existing",
				To:          "new-path",
				StorageName: "default",
			},
			errorCode: codes.OK,
		},
		{
			desc: "No from given",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "",
				To:          "new-path",
				StorageName: "default",
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "non-existing namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "non-existing",
				To:          "new-path",
				StorageName: "default",
			},
			errorCode: codes.InvalidArgument,
		},
		{
			desc: "existing destination namespace",
			request: &gitalypb.RenameNamespaceRequest{
				From:        "existing",
				To:          "existing",
				StorageName: "default",
			},
			errorCode: codes.InvalidArgument,
		},
	}

	_, err := client.AddNamespace(ctx, &gitalypb.AddNamespaceRequest{
		StorageName: "default",
		Name:        "existing",
	})
	require.NoError(t, err)

	for _, tc := range queries {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.RenameNamespace(ctx, tc.request)

			require.Equal(t, tc.errorCode, helper.GrpcCode(err))

			if tc.errorCode == codes.OK {
				client.RemoveNamespace(ctx, &gitalypb.RemoveNamespaceRequest{
					StorageName: "default",
					Name:        "new-path",
				})
			}
		})
	}
}
