package repository

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/archive"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func getSnapshot(t *testing.T, req *gitalypb.GetSnapshotRequest) ([]byte, error) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.GetSnapshot(ctx, req)
	if err != nil {
		return nil, err
	}

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetData(), err
	})

	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, reader)

	return buf.Bytes(), err
}

func touch(t *testing.T, format string, args ...interface{}) {
	path := fmt.Sprintf(format, args...)
	require.NoError(t, ioutil.WriteFile(path, nil, 0644))
}

func TestGetSnapshotSuccess(t *testing.T) {
	testRepo, repoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// Ensure certain files exist in the test repo.
	// CreateCommit produces a loose object with the given sha
	sha := testhelper.CreateCommit(t, repoPath, "master", nil)
	zeroes := strings.Repeat("0", 40)
	require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "hooks"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "objects/pack"), 0755))
	touch(t, filepath.Join(repoPath, "shallow"))
	touch(t, filepath.Join(repoPath, "objects/pack/pack-%s.pack"), zeroes)
	touch(t, filepath.Join(repoPath, "objects/pack/pack-%s.idx"), zeroes)
	touch(t, filepath.Join(repoPath, "objects/this-should-not-be-included"))

	req := &gitalypb.GetSnapshotRequest{Repository: testRepo}
	data, err := getSnapshot(t, req)
	require.NoError(t, err)

	entries, err := archive.TarEntries(bytes.NewReader(data))
	require.NoError(t, err)

	require.Contains(t, entries, "HEAD")
	require.Contains(t, entries, "packed-refs")
	require.Contains(t, entries, "refs/heads/")
	require.Contains(t, entries, "refs/tags/")
	require.Contains(t, entries, fmt.Sprintf("objects/%s/%s", sha[0:2], sha[2:40]))
	require.Contains(t, entries, "objects/pack/pack-"+zeroes+".idx")
	require.Contains(t, entries, "objects/pack/pack-"+zeroes+".pack")
	require.Contains(t, entries, "shallow")
	require.NotContains(t, entries, "objects/this-should-not-be-included")
	require.NotContains(t, entries, "config")
	require.NotContains(t, entries, "hooks/")
}

func TestGetSnapshotFailsIfRepositoryMissing(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	cleanupFn() // Remove the repo

	req := &gitalypb.GetSnapshotRequest{Repository: testRepo}
	data, err := getSnapshot(t, req)
	testhelper.RequireGrpcError(t, err, codes.NotFound)
	require.Empty(t, data)
}

func TestGetSnapshotFailsIfRepositoryContainsSymlink(t *testing.T) {
	testRepo, repoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// Make packed-refs into a symlink to break GetSnapshot()
	packedRefsFile := filepath.Join(repoPath, "packed-refs")
	require.NoError(t, os.Remove(packedRefsFile))
	require.NoError(t, os.Symlink("HEAD", packedRefsFile))

	req := &gitalypb.GetSnapshotRequest{Repository: testRepo}
	data, err := getSnapshot(t, req)
	testhelper.RequireGrpcError(t, err, codes.Internal)
	require.Contains(t, err.Error(), "Building snapshot failed")

	// At least some of the tar file should have been written so far
	require.NotEmpty(t, data)
}
