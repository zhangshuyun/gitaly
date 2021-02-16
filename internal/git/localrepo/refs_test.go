package localrepo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

const (
	masterOID      = git.ObjectID("1e292f8fedd741b75372e19097c76d327140c312")
	nonexistentOID = git.ObjectID("ba4f184e126b751d1bffad5897f263108befc780")
)

func TestRepo_ContainsRef(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := New(git.NewExecCommandFactory(config.Config), testRepo, config.Config)

	testcases := []struct {
		desc      string
		ref       string
		contained bool
	}{
		{
			desc:      "unqualified master branch",
			ref:       "master",
			contained: true,
		},
		{
			desc:      "fully qualified master branch",
			ref:       "refs/heads/master",
			contained: true,
		},
		{
			desc:      "nonexistent branch",
			ref:       "refs/heads/nonexistent",
			contained: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			contained, err := repo.HasRevision(ctx, git.Revision(tc.ref))
			require.NoError(t, err)
			require.Equal(t, tc.contained, contained)
		})
	}
}

func TestRepo_GetReference(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := New(git.NewExecCommandFactory(config.Config), testRepo, config.Config)

	testcases := []struct {
		desc        string
		ref         string
		expected    git.Reference
		expectedErr error
	}{
		{
			desc:     "fully qualified master branch",
			ref:      "refs/heads/master",
			expected: git.NewReference("refs/heads/master", masterOID.String()),
		},
		{
			desc:        "unqualified master branch fails",
			ref:         "master",
			expectedErr: git.ErrReferenceNotFound,
		},
		{
			desc:        "nonexistent branch",
			ref:         "refs/heads/nonexistent",
			expectedErr: git.ErrReferenceNotFound,
		},
		{
			desc:        "prefix returns an error",
			ref:         "refs/heads",
			expectedErr: git.ErrReferenceAmbiguous,
		},
		{
			desc:        "nonexistent branch",
			ref:         "nonexistent",
			expectedErr: git.ErrReferenceNotFound,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ref, err := repo.GetReference(ctx, git.ReferenceName(tc.ref))
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expected, ref)
		})
	}
}

func TestRepo_GetReferenceWithAmbiguousRefs(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	// Disable hooks
	defer func(oldValue string) {
		config.Config.Ruby.Dir = oldValue
	}(config.Config.Ruby.Dir)
	config.Config.Ruby.Dir = "/var/empty"

	repoProto, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := New(repoProto, config.Config)

	currentOID, err := repo.ResolveRevision(ctx, "refs/heads/master")
	require.NoError(t, err)

	prevOID, err := repo.ResolveRevision(ctx, "refs/heads/master~")
	require.NoError(t, err)

	for _, ref := range []git.ReferenceName{
		"refs/heads/something/master",
		"refs/heads/MASTER",
		"refs/heads/master2",
		"refs/heads/masterx",
		"refs/heads/refs/heads/master",
		"refs/heads/heads/master",
		"refs/master",
		"refs/tags/master",
	} {
		require.NoError(t, repo.UpdateRef(ctx, ref, prevOID, git.ZeroOID))
	}

	ref, err := repo.GetReference(ctx, "refs/heads/master")
	require.NoError(t, err)
	require.Equal(t, git.Reference{
		Name:   "refs/heads/master",
		Target: currentOID.String(),
	}, ref)
}

func TestRepo_GetReferences(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := New(git.NewExecCommandFactory(config.Config), testRepo, config.Config)

	testcases := []struct {
		desc    string
		pattern string
		match   func(t *testing.T, refs []git.Reference)
	}{
		{
			desc:    "master branch",
			pattern: "refs/heads/master",
			match: func(t *testing.T, refs []git.Reference) {
				require.Equal(t, []git.Reference{
					git.NewReference("refs/heads/master", masterOID.String()),
				}, refs)
			},
		},
		{
			desc:    "all references",
			pattern: "",
			match: func(t *testing.T, refs []git.Reference) {
				require.Len(t, refs, 94)
			},
		},
		{
			desc:    "branches",
			pattern: "refs/heads/",
			match: func(t *testing.T, refs []git.Reference) {
				require.Len(t, refs, 91)
			},
		},
		{
			desc:    "branches",
			pattern: "refs/heads/nonexistent",
			match: func(t *testing.T, refs []git.Reference) {
				require.Empty(t, refs)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			refs, err := repo.GetReferences(ctx, tc.pattern)
			require.NoError(t, err)
			tc.match(t, refs)
		})
	}
}

func TestRepo_GetBranches(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := New(git.NewExecCommandFactory(config.Config), testRepo, config.Config)

	refs, err := repo.GetBranches(ctx)
	require.NoError(t, err)
	require.Len(t, refs, 91)
}

func TestRepo_UpdateRef(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	defer func(oldValue string) {
		config.Config.Ruby.Dir = oldValue
	}(config.Config.Ruby.Dir)
	config.Config.Ruby.Dir = "/var/empty"

	gitCmdFactory := git.NewExecCommandFactory(config.Config)

	otherRef, err := New(gitCmdFactory, testRepo, config.Config).GetReference(ctx, "refs/heads/gitaly-test-ref")
	require.NoError(t, err)

	testcases := []struct {
		desc     string
		ref      string
		newValue git.ObjectID
		oldValue git.ObjectID
		verify   func(t *testing.T, repo *Repo, err error)
	}{
		{
			desc:     "successfully update master",
			ref:      "refs/heads/master",
			newValue: git.ObjectID(otherRef.Target),
			oldValue: masterOID,
			verify: func(t *testing.T, repo *Repo, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, otherRef.Target)
			},
		},
		{
			desc:     "update fails with stale oldValue",
			ref:      "refs/heads/master",
			newValue: git.ObjectID(otherRef.Target),
			oldValue: nonexistentOID,
			verify: func(t *testing.T, repo *Repo, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, masterOID.String())
			},
		},
		{
			desc:     "update fails with invalid newValue",
			ref:      "refs/heads/master",
			newValue: nonexistentOID,
			oldValue: masterOID,
			verify: func(t *testing.T, repo *Repo, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, masterOID.String())
			},
		},
		{
			desc:     "successfully update master with empty oldValue",
			ref:      "refs/heads/master",
			newValue: git.ObjectID(otherRef.Target),
			oldValue: "",
			verify: func(t *testing.T, repo *Repo, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, otherRef.Target)
			},
		},
		{
			desc:     "updating unqualified branch fails",
			ref:      "master",
			newValue: git.ObjectID(otherRef.Target),
			oldValue: masterOID,
			verify: func(t *testing.T, repo *Repo, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, masterOID.String())
			},
		},
		{
			desc:     "deleting master succeeds",
			ref:      "refs/heads/master",
			newValue: git.ZeroOID,
			oldValue: masterOID,
			verify: func(t *testing.T, repo *Repo, err error) {
				require.NoError(t, err)
				_, err = repo.GetReference(ctx, "refs/heads/master")
				require.Error(t, err)
			},
		},
		{
			desc:     "creating new branch succeeds",
			ref:      "refs/heads/new",
			newValue: masterOID,
			oldValue: git.ZeroOID,
			verify: func(t *testing.T, repo *Repo, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/new")
				require.NoError(t, err)
				require.Equal(t, ref.Target, masterOID.String())
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			// Re-create repo for each testcase.
			testRepo, _, cleanup := testhelper.NewTestRepo(t)
			defer cleanup()

			repo := New(gitCmdFactory, testRepo, config.Config)
			err := repo.UpdateRef(ctx, git.ReferenceName(tc.ref), tc.newValue, tc.oldValue)

			tc.verify(t, repo, err)
		})
	}
}
