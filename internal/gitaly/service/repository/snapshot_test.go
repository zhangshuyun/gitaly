package repository

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/archive"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
)

func getSnapshot(client gitalypb.RepositoryServiceClient, req *gitalypb.GetSnapshotRequest) ([]byte, error) {
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
	cfg, repo, repoPath, client := setupRepositoryService(t)

	// Ensure certain files exist in the test repo.
	// WriteCommit produces a loose object with the given sha
	sha := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
	zeroes := strings.Repeat("0", 40)
	require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "hooks"), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "objects/pack"), 0755))
	touch(t, filepath.Join(repoPath, "shallow"))
	touch(t, filepath.Join(repoPath, "objects/pack/pack-%s.pack"), zeroes)
	touch(t, filepath.Join(repoPath, "objects/pack/pack-%s.idx"), zeroes)
	touch(t, filepath.Join(repoPath, "objects/this-should-not-be-included"))

	req := &gitalypb.GetSnapshotRequest{Repository: repo}
	data, err := getSnapshot(client, req)
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
	for _, tc := range []struct {
		desc              string
		alternatePathFunc func(t *testing.T, storageDir, objDir string) string
	}{
		{
			desc:              "subdirectory",
			alternatePathFunc: func(*testing.T, string, string) string { return "./alt-objects" },
		},
		{
			desc: "absolute path",
			alternatePathFunc: func(t *testing.T, storageDir, objDir string) string {
				return filepath.Join(storageDir, gittest.NewObjectPoolName(t), "objects")
			},
		},
		{
			desc: "relative path",
			alternatePathFunc: func(t *testing.T, storageDir, objDir string) string {
				altObjDir, err := filepath.Rel(objDir, filepath.Join(
					storageDir, gittest.NewObjectPoolName(t), "objects",
				))
				require.NoError(t, err)
				return altObjDir
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, repoProto, repoPath, client := setupRepositoryServiceWithWorktree(t)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			ctx, cancel := testhelper.Context()
			defer cancel()

			const committerName = "Scrooge McDuck"
			const committerEmail = "scrooge@mcduck.com"

			cmd := exec.Command(cfg.Git.BinPath, "-C", repoPath,
				"-c", fmt.Sprintf("user.name=%s", committerName),
				"-c", fmt.Sprintf("user.email=%s", committerEmail),
				"commit", "--allow-empty", "-m", "An empty commit")
			alternateObjDir := tc.alternatePathFunc(t, cfg.Storages[0].Path, filepath.Join(repoPath, "objects"))
			commitSha := gittest.CreateCommitInAlternateObjectDirectory(t, cfg.Git.BinPath, repoPath, alternateObjDir, cmd)
			originalAlternatesCommit := string(commitSha)

			locator := config.NewLocator(cfg)
			catfileCache := catfile.NewCache(cfg)

			// ensure commit cannot be found in current repository
			c, err := catfileCache.BatchProcess(ctx, repo)
			require.NoError(t, err)
			_, err = c.Info(ctx, git.Revision(originalAlternatesCommit))
			require.True(t, catfile.IsNotFound(err))

			// write alternates file to point to alt objects folder
			alternatesPath, err := locator.InfoAlternatesPath(repoProto)
			require.NoError(t, err)
			require.NoError(t, ioutil.WriteFile(alternatesPath, []byte(filepath.Join(repoPath, ".git", fmt.Sprintf("%s\n", alternateObjDir))), 0644))

			// write another commit and ensure we can find it
			cmd = exec.Command(cfg.Git.BinPath, "-C", repoPath,
				"-c", fmt.Sprintf("user.name=%s", committerName),
				"-c", fmt.Sprintf("user.email=%s", committerEmail),
				"commit", "--allow-empty", "-m", "Another empty commit")
			commitSha = gittest.CreateCommitInAlternateObjectDirectory(t, cfg.Git.BinPath, repoPath, alternateObjDir, cmd)

			c, err = catfileCache.BatchProcess(ctx, repo)
			require.NoError(t, err)
			_, err = c.Info(ctx, git.Revision(commitSha))
			require.NoError(t, err)

			_, repoCopyPath, cleanupCopy := copyRepoUsingSnapshot(t, cfg, client, repoProto)
			defer cleanupCopy()

			// ensure the sha committed to the alternates directory can be accessed
			gittest.Exec(t, cfg, "-C", repoCopyPath, "cat-file", "-p", originalAlternatesCommit)
			gittest.Exec(t, cfg, "-C", repoCopyPath, "fsck")
		})
	}
}

func TestGetSnapshotWithDedupeSoftFailures(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	testRepo, repoPath, cleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg, cfg.Storages[0])
	defer cleanup()

	locator := config.NewLocator(cfg)

	// write alternates file to point to alternates objects folder that doesn't exist
	alternateObjDir := "./alt-objects"
	alternateObjPath := filepath.Join(repoPath, ".git", alternateObjDir)
	alternatesPath, err := locator.InfoAlternatesPath(testRepo)
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(alternatesPath, []byte(fmt.Sprintf("%s\n", alternateObjPath)), 0644))

	req := &gitalypb.GetSnapshotRequest{Repository: testRepo}
	_, err = getSnapshot(client, req)
	assert.NoError(t, err)
	require.NoError(t, os.Remove(alternatesPath))

	// write alternates file to point outside storage root
	storageRoot, err := locator.GetStorageByName(testRepo.GetStorageName())
	require.NoError(t, err)
	require.NoError(t, ioutil.WriteFile(alternatesPath, []byte(filepath.Join(storageRoot, "..")), 0600))

	_, err = getSnapshot(client, &gitalypb.GetSnapshotRequest{Repository: testRepo})
	assert.NoError(t, err)
	require.NoError(t, os.Remove(alternatesPath))

	// write alternates file with bad permissions
	require.NoError(t, ioutil.WriteFile(alternatesPath, []byte(fmt.Sprintf("%s\n", alternateObjPath)), 0000))
	_, err = getSnapshot(client, req)
	assert.NoError(t, err)
	require.NoError(t, os.Remove(alternatesPath))

	// write alternates file without newline
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	cmd := exec.Command(cfg.Git.BinPath, "-C", repoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", "An empty commit")

	commitSha := gittest.CreateCommitInAlternateObjectDirectory(t, cfg.Git.BinPath, repoPath, alternateObjDir, cmd)
	originalAlternatesCommit := string(commitSha)

	require.NoError(t, ioutil.WriteFile(alternatesPath, []byte(alternateObjPath), 0644))

	_, repoCopyPath, cleanupCopy := copyRepoUsingSnapshot(t, cfg, client, testRepo)
	defer cleanupCopy()

	// ensure the sha committed to the alternates directory can be accessed
	gittest.Exec(t, cfg, "-C", repoCopyPath, "cat-file", "-p", originalAlternatesCommit)
	gittest.Exec(t, cfg, "-C", repoCopyPath, "fsck")
}

// copyRepoUsingSnapshot creates a tarball snapshot, then creates a new repository from that snapshot
func copyRepoUsingSnapshot(t *testing.T, cfg config.Cfg, client gitalypb.RepositoryServiceClient, source *gitalypb.Repository) (*gitalypb.Repository, string, func()) {
	t.Helper()
	// create the tar
	req := &gitalypb.GetSnapshotRequest{Repository: source}
	data, err := getSnapshot(client, req)
	require.NoError(t, err)

	secret := "my secret"
	srv := httptest.NewServer(&tarTesthandler{tarData: bytes.NewBuffer(data), secret: secret})
	defer srv.Close()

	repoCopy, repoCopyPath, cleanupCopy := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], "copy")

	// Delete the repository so we can re-use the path
	require.NoError(t, os.RemoveAll(repoCopyPath))

	createRepoReq := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: repoCopy,
		HttpUrl:    srv.URL + tarPath,
		HttpAuth:   secret,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()
	rsp, err := client.CreateRepositoryFromSnapshot(ctx, createRepoReq)
	require.NoError(t, err)
	testassert.ProtoEqual(t, rsp, &gitalypb.CreateRepositoryFromSnapshotResponse{})
	return repoCopy, repoCopyPath, cleanupCopy
}

func TestGetSnapshotFailsIfRepositoryMissing(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)
	repo := &gitalypb.Repository{
		StorageName:   cfg.Storages[0].Name,
		RelativePath:  t.Name(),
		GlRepository:  gittest.GlRepository,
		GlProjectPath: gittest.GlProjectPath,
	}

	req := &gitalypb.GetSnapshotRequest{Repository: repo}
	data, err := getSnapshot(client, req)
	testhelper.RequireGrpcError(t, err, codes.NotFound)
	require.Empty(t, data)
}

func TestGetSnapshotFailsIfRepositoryContainsSymlink(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	// Make packed-refs into a symlink to break GetSnapshot()
	packedRefsFile := filepath.Join(repoPath, "packed-refs")
	require.NoError(t, os.Remove(packedRefsFile))
	require.NoError(t, os.Symlink("HEAD", packedRefsFile))

	req := &gitalypb.GetSnapshotRequest{Repository: repo}
	data, err := getSnapshot(client, req)
	testhelper.RequireGrpcError(t, err, codes.Internal)
	require.Contains(t, err.Error(), "building snapshot failed")

	// At least some of the tar file should have been written so far
	require.NotEmpty(t, data)
}
