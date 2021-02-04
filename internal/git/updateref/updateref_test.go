package updateref

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

func setup(t *testing.T) (context.Context, *gitalypb.Repository, string, func()) {
	ctx, cancel := testhelper.Context()
	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	teardown := func() {
		cancel()
		cleanup()
	}

	return ctx, testRepo, testRepoPath, teardown
}

func TestCreate(t *testing.T) {
	ctx, testRepo, _, teardown := setup(t)
	defer teardown()

	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	headCommit, err := log.GetCommit(ctx, gitCmdFactory, testRepo, "HEAD")
	require.NoError(t, err)

	updater, err := New(ctx, config.Config, testRepo)
	require.NoError(t, err)

	ref := git.ReferenceName("refs/heads/_create")
	sha := headCommit.Id

	require.NoError(t, updater.Create(ref, sha))
	require.NoError(t, updater.Wait())

	// check the ref was created
	commit, logErr := log.GetCommit(ctx, gitCmdFactory, testRepo, ref.Revision())
	require.NoError(t, logErr)
	require.Equal(t, commit.Id, sha, "reference was created with the wrong SHA")
}

func TestUpdate(t *testing.T) {
	ctx, testRepo, _, teardown := setup(t)
	defer teardown()

	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	headCommit, err := log.GetCommit(ctx, gitCmdFactory, testRepo, "HEAD")
	require.NoError(t, err)

	updater, err := New(ctx, config.Config, testRepo)
	require.NoError(t, err)

	ref := git.ReferenceName("refs/heads/feature")
	sha := headCommit.Id

	// Sanity check: ensure the ref exists before we start
	commit, logErr := log.GetCommit(ctx, gitCmdFactory, testRepo, ref.Revision())
	require.NoError(t, logErr)
	require.NotEqual(t, commit.Id, sha, "%s points to HEAD: %s in the test repository", ref.String(), sha)

	require.NoError(t, updater.Update(ref, sha, ""))
	require.NoError(t, updater.Wait())

	// check the ref was updated
	commit, logErr = log.GetCommit(ctx, gitCmdFactory, testRepo, ref.Revision())
	require.NoError(t, logErr)
	require.Equal(t, commit.Id, sha, "reference was not updated")

	// since ref has been updated to HEAD, we know that it does not point to HEAD^. So, HEAD^ is an invalid "old value" for updating ref
	parentCommit, err := log.GetCommit(ctx, gitCmdFactory, testRepo, "HEAD^")
	require.NoError(t, err)
	require.Error(t, updater.Update(ref, parentCommit.Id, parentCommit.Id))

	// check the ref was not updated
	commit, logErr = log.GetCommit(ctx, gitCmdFactory, testRepo, ref.Revision())
	require.NoError(t, logErr)
	require.NotEqual(t, commit.Id, parentCommit.Id, "reference was updated when it shouldn't have been")
}

func TestDelete(t *testing.T) {
	ctx, testRepo, _, teardown := setup(t)
	defer teardown()

	updater, err := New(ctx, config.Config, testRepo)
	require.NoError(t, err)

	ref := git.ReferenceName("refs/heads/feature")

	require.NoError(t, updater.Delete(ref))
	require.NoError(t, updater.Wait())

	// check the ref was removed
	_, err = log.GetCommit(ctx, git.NewExecCommandFactory(config.Config), testRepo, ref.Revision())
	require.True(t, log.IsNotFound(err), "expected 'not found' error got %v", err)
}

func TestBulkOperation(t *testing.T) {
	ctx, testRepo, _, teardown := setup(t)
	defer teardown()

	headCommit, err := log.GetCommit(ctx, git.NewExecCommandFactory(config.Config), testRepo, "HEAD")
	require.NoError(t, err)

	updater, err := New(ctx, config.Config, testRepo)
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		ref := fmt.Sprintf("refs/head/_test_%d", i)
		require.NoError(t, updater.Create(git.ReferenceName(ref), headCommit.Id), "Failed to create ref %d", i)
	}

	require.NoError(t, updater.Wait())

	refs, err := localrepo.New(testRepo, config.Config).GetReferences(ctx, "refs/")
	require.NoError(t, err)
	require.Greater(t, len(refs), 1000, "At least 1000 refs should be present")
}

func TestContextCancelAbortsRefChanges(t *testing.T) {
	ctx, testRepo, _, teardown := setup(t)
	defer teardown()

	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	headCommit, err := log.GetCommit(ctx, gitCmdFactory, testRepo, "HEAD")
	require.NoError(t, err)

	childCtx, childCancel := context.WithCancel(ctx)
	updater, err := New(childCtx, config.Config, testRepo)
	require.NoError(t, err)

	ref := git.ReferenceName("refs/heads/_shouldnotexist")

	require.NoError(t, updater.Create(ref, headCommit.Id))

	// Force the update-ref process to terminate early
	childCancel()
	require.Error(t, updater.Wait())

	// check the ref doesn't exist
	_, err = log.GetCommit(ctx, gitCmdFactory, testRepo, ref.Revision())
	require.True(t, log.IsNotFound(err), "expected 'not found' error got %v", err)
}

func TestUpdater_closingStdinAbortsChanges(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	headCommit, err := log.GetCommit(ctx, gitCmdFactory, testRepo, "HEAD")
	require.NoError(t, err)

	ref := git.ReferenceName("refs/heads/shouldnotexist")

	updater, err := New(ctx, config.Config, testRepo)
	require.NoError(t, err)
	require.NoError(t, updater.Create(ref, headCommit.Id))

	// Note that we call `Wait()` on the command, not on the updater. This
	// circumvents our usual semantics of sending "commit" and thus
	// emulates that the command somehow terminates correctly without us
	// terminating it intentionally. Previous to our use of the "start"
	// verb, this would've caused the reference to be created...
	require.NoError(t, updater.cmd.Wait())

	// ... but as we now use explicit transactional behaviour, this is no
	// longer the case.
	_, err = log.GetCommit(ctx, gitCmdFactory, testRepo, ref.Revision())
	require.True(t, log.IsNotFound(err), "expected 'not found' error got %v", err)
}
