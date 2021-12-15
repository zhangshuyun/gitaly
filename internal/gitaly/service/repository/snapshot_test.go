package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/archive"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
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
	require.NoError(t, os.WriteFile(path, nil, 0o644))
}

func TestGetSnapshotSuccess(t *testing.T) {
	t.Parallel()
	cfg, repo, repoPath, client := setupRepositoryService(t)

	// Ensure certain files exist in the test repo.
	// WriteCommit produces a loose object with the given sha
	sha := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))
	zeroes := strings.Repeat("0", 40)
	require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "hooks"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "objects/pack"), 0o755))
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
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testGetSnapshotWithDedupe)
}

func testGetSnapshotWithDedupe(t *testing.T, ctx context.Context) {
	for _, tc := range []struct {
		desc              string
		alternatePathFunc func(t *testing.T, storageDir, repoPath string) string
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
			cfg, repoProto, repoPath, client := setupRepositoryService(t)

			alternateObjDir := tc.alternatePathFunc(t, cfg.Storages[0].Path, filepath.Join(repoPath, "objects"))
			absoluteAlternateObjDir := alternateObjDir
			if !filepath.IsAbs(alternateObjDir) {
				absoluteAlternateObjDir = filepath.Join(repoPath, "objects", alternateObjDir)
			}

			firstCommitID := gittest.WriteCommit(t, cfg, repoPath,
				gittest.WithMessage("An empty commit"),
				gittest.WithAlternateObjectDirectory(absoluteAlternateObjDir),
			)

			locator := config.NewLocator(cfg)

			// We haven't yet written the alternates file, and thus we shouldn't be able
			// to find this commit yet.
			gittest.RequireObjectNotExists(t, cfg, repoPath, firstCommitID)

			// Write alternates file to point to alt objects folder.
			alternatesPath, err := locator.InfoAlternatesPath(repoProto)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(alternatesPath, []byte(fmt.Sprintf("%s\n", alternateObjDir)), 0o644))

			// Write another commit into the alternate object directory.
			secondCommitID := gittest.WriteCommit(t, cfg, repoPath,
				gittest.WithMessage("Another empty commit"),
				gittest.WithAlternateObjectDirectory(absoluteAlternateObjDir),
			)

			// We should now be able to find both commits given that the alternates file
			// points to the object directory we've created them in.
			gittest.RequireObjectExists(t, cfg, repoPath, firstCommitID)
			gittest.RequireObjectExists(t, cfg, repoPath, secondCommitID)

			repoCopy, _ := copyRepoUsingSnapshot(t, ctx, cfg, client, repoProto)
			repoCopy.RelativePath = getReplicaPath(ctx, t, client, repoCopy)
			repoCopyPath, err := locator.GetRepoPath(repoCopy)
			require.NoError(t, err)

			// ensure the sha committed to the alternates directory can be accessed
			gittest.Exec(t, cfg, "-C", repoCopyPath, "cat-file", "-p", firstCommitID.String())
			gittest.Exec(t, cfg, "-C", repoCopyPath, "fsck")
		})
	}
}

func TestGetSnapshot_alternateObjectDirectory(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.TxAtomicRepositoryCreation).Run(t, testGetSnapshotAlternateObjectDirectory)
}

func testGetSnapshotAlternateObjectDirectory(t *testing.T, ctx context.Context) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

	locator := config.NewLocator(cfg)
	alternatesFile, err := locator.InfoAlternatesPath(repo)
	require.NoError(t, err)

	req := &gitalypb.GetSnapshotRequest{Repository: repo}

	t.Run("nonexistent", func(t *testing.T) {
		alternateObjectDir := filepath.Join(repoPath, "does-not-exist")

		require.NoError(t, os.WriteFile(alternatesFile, []byte(fmt.Sprintf("%s\n", alternateObjectDir)), 0o644))
		defer func() {
			require.NoError(t, os.Remove(alternatesFile))
		}()

		_, err = getSnapshot(client, req)
		require.NoError(t, err)
	})

	t.Run("escape storage root", func(t *testing.T) {
		storageRoot, err := locator.GetStorageByName(repo.GetStorageName())
		require.NoError(t, err)

		alternateObjectDir := filepath.Join(storageRoot, "..")

		require.NoError(t, os.WriteFile(alternatesFile, []byte(alternateObjectDir), 0o600))
		defer func() {
			require.NoError(t, os.Remove(alternatesFile))
		}()

		_, err = getSnapshot(client, &gitalypb.GetSnapshotRequest{Repository: repo})
		require.NoError(t, err)
	})

	t.Run("bad permissions", func(t *testing.T) {
		alternateObjectDir := filepath.Join(repoPath, "bad-permissions")

		require.NoError(t, os.WriteFile(alternatesFile, []byte(fmt.Sprintf("%s\n", alternateObjectDir)), 0o000))
		defer func() {
			require.NoError(t, os.Remove(alternatesFile))
		}()

		_, err = getSnapshot(client, req)
		require.NoError(t, err)
	})

	t.Run("valid alternate object directory", func(t *testing.T) {
		alternateObjectDir := filepath.Join(repoPath, "valid-odb")

		commitID := gittest.WriteCommit(t, cfg, repoPath,
			gittest.WithAlternateObjectDirectory(alternateObjectDir),
			// Create a branch with the commit such that the snapshot would indeed treat
			// this commit as referenced.
			gittest.WithBranch("some-branch"),
		)

		require.NoError(t, os.WriteFile(alternatesFile, []byte(alternateObjectDir), 0o644))
		defer func() {
			require.NoError(t, os.Remove(alternatesFile))
		}()

		repoCopy, _ := copyRepoUsingSnapshot(t, ctx, cfg, client, repo)
		repoCopy.RelativePath = getReplicaPath(ctx, t, client, repoCopy)
		repoCopyPath, err := locator.GetRepoPath(repoCopy)
		require.NoError(t, err)

		// Ensure the object committed to the alternates directory can be accessed and that
		// the repository is consistent.
		gittest.Exec(t, cfg, "-C", repoCopyPath, "cat-file", "-p", commitID.String())
		gittest.Exec(t, cfg, "-C", repoCopyPath, "fsck")
	})
}

// copyRepoUsingSnapshot creates a tarball snapshot, then creates a new repository from that snapshot
func copyRepoUsingSnapshot(t *testing.T, ctx context.Context, cfg config.Cfg, client gitalypb.RepositoryServiceClient, source *gitalypb.Repository) (*gitalypb.Repository, string) {
	t.Helper()
	// create the tar
	req := &gitalypb.GetSnapshotRequest{Repository: source}
	data, err := getSnapshot(client, req)
	require.NoError(t, err)

	secret := "my secret"
	srv := httptest.NewServer(&tarTesthandler{tarData: bytes.NewBuffer(data), secret: secret})
	defer srv.Close()

	repoCopy, repoCopyPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

	// Delete the repository so we can re-use the path
	require.NoError(t, os.RemoveAll(repoCopyPath))

	createRepoReq := &gitalypb.CreateRepositoryFromSnapshotRequest{
		Repository: repoCopy,
		HttpUrl:    srv.URL + tarPath,
		HttpAuth:   secret,
	}

	rsp, err := client.CreateRepositoryFromSnapshot(ctx, createRepoReq)
	require.NoError(t, err)
	testhelper.ProtoEqual(t, rsp, &gitalypb.CreateRepositoryFromSnapshotResponse{})
	return repoCopy, repoCopyPath
}

func TestGetSnapshotFailsIfRepositoryMissing(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)
	repo := &gitalypb.Repository{
		StorageName:   cfg.Storages[0].Name,
		RelativePath:  t.Name(),
		GlRepository:  gittest.GlRepository,
		GlProjectPath: gittest.GlProjectPath,
	}

	req := &gitalypb.GetSnapshotRequest{Repository: repo}
	data, err := getSnapshot(client, req)
	testhelper.RequireGrpcCode(t, err, codes.NotFound)
	require.Empty(t, data)
}

func TestGetSnapshotFailsIfRepositoryContainsSymlink(t *testing.T) {
	t.Parallel()
	_, repo, repoPath, client := setupRepositoryService(t)

	// Make packed-refs into a symlink to break GetSnapshot()
	packedRefsFile := filepath.Join(repoPath, "packed-refs")
	require.NoError(t, os.Remove(packedRefsFile))
	require.NoError(t, os.Symlink("HEAD", packedRefsFile))

	req := &gitalypb.GetSnapshotRequest{Repository: repo}
	data, err := getSnapshot(client, req)
	testhelper.RequireGrpcCode(t, err, codes.Internal)
	require.Contains(t, err.Error(), "building snapshot failed")

	// At least some of the tar file should have been written so far
	require.NotEmpty(t, data)
}
