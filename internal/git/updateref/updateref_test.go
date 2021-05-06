package updateref

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	hooks.Override = "/"
	return m.Run()
}

func setupUpdater(t *testing.T, ctx context.Context) (config.Cfg, *localrepo.Repo, *Updater) {
	t.Helper()

	cfg, protoRepo, _ := testcfg.BuildWithRepo(t)

	gitCmdFactory := git.NewExecCommandFactory(cfg)
	repo := localrepo.NewTestRepo(t, cfg, protoRepo)

	updater, err := New(ctx, cfg, gitCmdFactory, repo)
	require.NoError(t, err)

	return cfg, repo, updater
}

func TestCreate(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repo, updater := setupUpdater(t, ctx)

	headCommit, err := repo.ReadCommit(ctx, "HEAD")
	require.NoError(t, err)

	ref := git.ReferenceName("refs/heads/_create")
	sha := headCommit.Id

	require.NoError(t, updater.Create(ref, sha))
	require.NoError(t, updater.Wait())

	// check the ref was created
	commit, logErr := repo.ReadCommit(ctx, ref.Revision())
	require.NoError(t, logErr)
	require.Equal(t, commit.Id, sha, "reference was created with the wrong SHA")
}

func TestUpdate(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repo, updater := setupUpdater(t, ctx)

	headCommit, err := repo.ReadCommit(ctx, "HEAD")
	require.NoError(t, err)

	ref := git.ReferenceName("refs/heads/feature")
	sha := headCommit.Id

	// Sanity check: ensure the ref exists before we start
	commit, logErr := repo.ReadCommit(ctx, ref.Revision())
	require.NoError(t, logErr)
	require.NotEqual(t, commit.Id, sha, "%s points to HEAD: %s in the test repository", ref.String(), sha)

	require.NoError(t, updater.Update(ref, sha, ""))
	require.NoError(t, updater.Prepare())
	require.NoError(t, updater.Wait())

	// check the ref was updated
	commit, logErr = repo.ReadCommit(ctx, ref.Revision())
	require.NoError(t, logErr)
	require.Equal(t, commit.Id, sha, "reference was not updated")

	// since ref has been updated to HEAD, we know that it does not point to HEAD^. So, HEAD^ is an invalid "old value" for updating ref
	parentCommit, err := repo.ReadCommit(ctx, "HEAD^")
	require.NoError(t, err)
	require.Error(t, updater.Update(ref, parentCommit.Id, parentCommit.Id))

	// check the ref was not updated
	commit, logErr = repo.ReadCommit(ctx, ref.Revision())
	require.NoError(t, logErr)
	require.NotEqual(t, commit.Id, parentCommit.Id, "reference was updated when it shouldn't have been")
}

func TestDelete(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repo, updater := setupUpdater(t, ctx)

	ref := git.ReferenceName("refs/heads/feature")

	require.NoError(t, updater.Delete(ref))
	require.NoError(t, updater.Wait())

	// check the ref was removed
	_, err := repo.ReadCommit(ctx, ref.Revision())
	require.Equal(t, localrepo.ErrObjectNotFound, err, "expected 'not found' error got %v", err)
}

func TestUpdater_prepareLocksTransaction(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repo, updater := setupUpdater(t, ctx)

	commit, logErr := repo.ReadCommit(ctx, "refs/heads/master")
	require.NoError(t, logErr)

	require.NoError(t, updater.Update("refs/heads/feature", commit.Id, ""))
	require.NoError(t, updater.Prepare())
	require.NoError(t, updater.Update("refs/heads/feature", commit.Id, ""))

	err := updater.Wait()
	require.Error(t, err, "cannot update after prepare")
	require.Contains(t, err.Error(), "fatal: prepared transactions can only be closed")
}

func TestBulkOperation(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repo, updater := setupUpdater(t, ctx)

	headCommit, err := repo.ReadCommit(ctx, "HEAD")
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		ref := fmt.Sprintf("refs/head/_test_%d", i)
		require.NoError(t, updater.Create(git.ReferenceName(ref), headCommit.Id), "Failed to create ref %d", i)
	}

	require.NoError(t, updater.Wait())

	refs, err := repo.GetReferences(ctx, "refs/")
	require.NoError(t, err)
	require.Greater(t, len(refs), 1000, "At least 1000 refs should be present")
}

func TestContextCancelAbortsRefChanges(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repo, _ := setupUpdater(t, ctx)

	headCommit, err := repo.ReadCommit(ctx, "HEAD")
	require.NoError(t, err)

	childCtx, childCancel := context.WithCancel(ctx)
	updater, err := New(childCtx, cfg, git.NewExecCommandFactory(cfg), repo)
	require.NoError(t, err)

	ref := git.ReferenceName("refs/heads/_shouldnotexist")

	require.NoError(t, updater.Create(ref, headCommit.Id))

	// Force the update-ref process to terminate early
	childCancel()
	require.Error(t, updater.Wait())

	// check the ref doesn't exist
	_, err = repo.ReadCommit(ctx, ref.Revision())
	require.Equal(t, localrepo.ErrObjectNotFound, err, "expected 'not found' error got %v", err)
}

func TestUpdater_closingStdinAbortsChanges(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repo, updater := setupUpdater(t, ctx)

	headCommit, err := repo.ReadCommit(ctx, "HEAD")
	require.NoError(t, err)

	ref := git.ReferenceName("refs/heads/shouldnotexist")

	require.NoError(t, updater.Create(ref, headCommit.Id))

	// Note that we call `Wait()` on the command, not on the updater. This
	// circumvents our usual semantics of sending "commit" and thus
	// emulates that the command somehow terminates correctly without us
	// terminating it intentionally. Previous to our use of the "start"
	// verb, this would've caused the reference to be created...
	require.NoError(t, updater.cmd.Wait())

	// ... but as we now use explicit transactional behaviour, this is no
	// longer the case.
	_, err = repo.ReadCommit(ctx, ref.Revision())
	require.Equal(t, localrepo.ErrObjectNotFound, err, "expected 'not found' error got %v", err)
}

func TestUpdater_capturesStderr(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, _, updater := setupUpdater(t, ctx)

	ref := "refs/heads/a"
	newValue := strings.Repeat("1", 40)
	oldValue := git.ZeroOID.String()

	require.NoError(t, updater.Update(git.ReferenceName(ref), newValue, oldValue))

	expectedErr := fmt.Sprintf("git update-ref: exit status 128, stderr: "+
		"\"fatal: commit: cannot update ref '%s': "+
		"trying to write ref '%s' with nonexistent object %s\\n\"", ref, ref, newValue)

	err := updater.Wait()
	require.NotNil(t, err)
	require.Equal(t, err.Error(), expectedErr)
}
