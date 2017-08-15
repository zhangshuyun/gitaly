package repository

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

func TestGarbageCollectSuccess(t *testing.T) {
	server := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t)
	defer conn.Close()

	tests := []struct {
		req  *pb.GarbageCollectRequest
		desc string
	}{
		{
			req:  &pb.GarbageCollectRequest{Repository: testRepo, CreateBitmap: false},
			desc: "without bitmap",
		},
		{
			req:  &pb.GarbageCollectRequest{Repository: testRepo, CreateBitmap: true},
			desc: "with bitmap",
		},
	}

	packPath := path.Join(testhelper.GitlabTestStoragePath(), testRepo.GetRelativePath(), "objects", "pack")

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Reset mtime to a long while ago since some filesystems don't have sub-second
			// precision on `mtime`.
			// Stamp taken from https://golang.org/pkg/time/#pkg-constants
			testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, packPath)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.GarbageCollect(ctx, test.req)
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

func TestGarbageCollectFailure(t *testing.T) {
	server := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t)
	defer conn.Close()

	tests := []struct {
		repo *pb.Repository
		code codes.Code
	}{
		{repo: nil, code: codes.InvalidArgument},
		{repo: &pb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{repo: &pb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{repo: &pb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.repo), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.GarbageCollect(ctx, &pb.GarbageCollectRequest{Repository: test.repo})
			testhelper.AssertGrpcError(t, err, test.code, "")
		})
	}

}
