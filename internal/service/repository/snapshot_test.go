package repository

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/archive"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func getSnapshot(t *testing.T, req *gitalypb.GetSnapshotRequest) ([]byte, error) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

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

func TestGetSnapshotWithDedupe(t *testing.T) {
	testRepo, repoPath, cleanup := testhelper.NewTestRepoWithWorktree(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	cmd := exec.Command("git", "-C", repoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", "An empty commit")
	alternateObjDir := "./alt-objects"
	commitSha := testhelper.CreateCommitInAlternateObjectDirectory(t, repoPath, alternateObjDir, cmd)
	originalAlternatesCommit := string(commitSha)

	// ensure commit cannot be found in current repository
	c, err := catfile.New(ctx, testRepo)
	require.NoError(t, err)
	_, err = c.Info(originalAlternatesCommit)
	require.True(t, catfile.IsNotFound(err))

	// write alternates file to point to alt objects folder
	alternatesPath, err := git.InfoAlternatesPath(testRepo)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(alternatesPath, []byte(path.Join(repoPath, ".git", fmt.Sprintf("%s\n", alternateObjDir))), 0644))

	// write another commit and ensure we can find it
	cmd = exec.Command("git", "-C", repoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", "Another empty commit")
	commitSha = testhelper.CreateCommitInAlternateObjectDirectory(t, repoPath, alternateObjDir, cmd)

	c, err = catfile.New(ctx, testRepo)
	require.NoError(t, err)
	_, err = c.Info(string(commitSha))
	require.NoError(t, err)

	_, repoCopyPath, cleanupCopy := copyRepoUsingSnapshot(t, testRepo)
	defer cleanupCopy()

	// ensure the sha committed to the alternates directory can be accessed
	testhelper.MustRunCommand(t, nil, "git", "-C", repoCopyPath, "cat-file", "-p", originalAlternatesCommit)
	testhelper.MustRunCommand(t, nil, "git", "-C", repoCopyPath, "fsck")
}

func TestGetSnapshotWithDedupeSoftFailures(t *testing.T) {
	testRepo, repoPath, cleanup := testhelper.NewTestRepoWithWorktree(t)
	defer cleanup()

	// write alternates file to point to alternates objects folder that doesn't exist
	alternateObjDir := "./alt-objects"
	alternateObjPath := path.Join(repoPath, ".git", alternateObjDir)
	alternatesPath, err := git.InfoAlternatesPath(testRepo)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(alternatesPath, []byte(fmt.Sprintf("%s\n", alternateObjPath)), 0644))

	req := &gitalypb.GetSnapshotRequest{Repository: testRepo}
	_, err = getSnapshot(t, req)
	assert.NoError(t, err)
	require.NoError(t, os.Remove(alternatesPath))
	// write alternates file with bad permissions
	require.NoError(t, ioutil.WriteFile(alternatesPath, []byte(fmt.Sprintf("%s\n", alternateObjPath)), 0000))
	_, err = getSnapshot(t, req)
	assert.NoError(t, err)
	require.NoError(t, os.Remove(alternatesPath))

	// write alternates file without newline
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	cmd := exec.Command("git", "-C", repoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", "An empty commit")

	commitSha := testhelper.CreateCommitInAlternateObjectDirectory(t, repoPath, alternateObjDir, cmd)
	originalAlternatesCommit := string(commitSha)

	require.NoError(t, ioutil.WriteFile(alternatesPath, []byte(alternateObjPath), 0644))

	_, repoCopyPath, cleanupCopy := copyRepoUsingSnapshot(t, testRepo)
	defer cleanupCopy()

	// ensure the sha committed to the alternates directory can be accessed
	testhelper.MustRunCommand(t, nil, "git", "-C", repoCopyPath, "cat-file", "-p", originalAlternatesCommit)
	testhelper.MustRunCommand(t, nil, "git", "-C", repoCopyPath, "fsck")
}

// copyRepoUsingSnapshot creates a tarball snapshot, then creates a new repository from that snapshot
func copyRepoUsingSnapshot(t *testing.T, source *gitalypb.Repository) (*gitalypb.Repository, string, func()) {
	// create the tar
	req := &gitalypb.GetSnapshotRequest{Repository: source}
	data, err := getSnapshot(t, req)
	require.NoError(t, err)

	secret := "my secret"
	srv := httptest.NewServer(&tarTesthandler{tarData: bytes.NewBuffer(data), secret: secret})
	defer srv.Close()

	repoCopy, repoCopyPath, cleanupCopy := testhelper.NewTestRepo(t)

	// Delete the repository so we can re-use the path
	require.NoError(t, os.RemoveAll(repoCopyPath))

	createRepoReq := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: repoCopy,
		HttpUrl:    srv.URL + tarPath,
		HttpAuth:   secret,
	}

	rsp, err := createFromSnapshot(t, createRepoReq)
	require.NoError(t, err)
	require.Equal(t, rsp, &gitalypb.CreateRepositoryFromSnapshotResponse{})
	return repoCopy, repoCopyPath, cleanupCopy
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
	require.Contains(t, err.Error(), "building snapshot failed")

	// At least some of the tar file should have been written so far
	require.NotEmpty(t, data)
}
