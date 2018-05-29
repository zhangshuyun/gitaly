package repository

import (
	"context"
	"path"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

func TestRepackIncrementalSuccess(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	packPath := path.Join(testhelper.GitlabTestStoragePath(), testRepo.GetRelativePath(), "objects", "pack")

	// Reset mtime to a long while ago since some filesystems don't have sub-second
	// precision on `mtime`.
	// Stamp taken from https://golang.org/pkg/time/#pkg-constants
	testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, path.Join(packPath, "*"))
	testTime := time.Date(2006, 01, 02, 15, 04, 05, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.RepackIncremental(ctx, &pb.RepackIncrementalRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// Entire `path`-folder gets updated so this is fine :D
	assertModTimeAfter(t, testTime, packPath)
}

func TestRepackIncrementalFailure(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		repo *pb.Repository
		code codes.Code
		desc string
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "invalid storage name", repo: &pb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "no storage name", repo: &pb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{desc: "non-existing repo", repo: &pb.Repository{StorageName: testhelper.TestRepository().GetStorageName(), RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.RepackIncremental(ctx, &pb.RepackIncrementalRequest{Repository: test.repo})
			testhelper.RequireGrpcError(t, err, test.code)
		})
	}
}

func TestRepackFullSuccess(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tests := []struct {
		req  *pb.RepackFullRequest
		desc string
	}{
		{req: &pb.RepackFullRequest{Repository: testRepo, CreateBitmap: true}, desc: "with bitmap"},
		{req: &pb.RepackFullRequest{Repository: testRepo, CreateBitmap: false}, desc: "without bitmap"},
	}

	packPath := path.Join(testRepoPath, "objects", "pack")

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Reset mtime to a long while ago since some filesystems don't have sub-second
			// precision on `mtime`.
			testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, packPath)
			testTime := time.Date(2006, 01, 02, 15, 04, 05, 0, time.UTC)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.RepackFull(ctx, test.req)
			assert.NoError(t, err)
			assert.NotNil(t, c)

			// Entire `path`-folder gets updated so this is fine :D
			assertModTimeAfter(t, testTime, packPath)

			bmPath, err := filepath.Glob(path.Join(packPath, "pack-*.bitmap"))
			if err != nil {
				t.Fatalf("Error globbing bitmaps: %v", err)
			}
			if test.req.GetCreateBitmap() {
				if len(bmPath) == 0 {
					t.Errorf("No bitmaps found")
				}
			} else {
				if len(bmPath) != 0 {
					t.Errorf("Bitmap found: %v", bmPath)
				}
			}
		})
	}
}

func TestRepackFullFailure(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		repo *pb.Repository
		code codes.Code
		desc string
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "invalid storage name", repo: &pb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "no storage name", repo: &pb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{desc: "non-existing repo", repo: &pb.Repository{StorageName: testhelper.TestRepository().GetStorageName(), RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.RepackFull(ctx, &pb.RepackFullRequest{Repository: test.repo})
			testhelper.RequireGrpcError(t, err, test.code)
		})
	}
}
