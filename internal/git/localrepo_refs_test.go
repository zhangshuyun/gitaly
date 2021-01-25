package git

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

const (
	masterOID      = ObjectID("1e292f8fedd741b75372e19097c76d327140c312")
	nonexistentOID = ObjectID("ba4f184e126b751d1bffad5897f263108befc780")
)

func TestLocalRepository_ContainsRef(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := NewRepository(testRepo, config.Config)

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
			contained, err := repo.HasRevision(ctx, Revision(tc.ref))
			require.NoError(t, err)
			require.Equal(t, tc.contained, contained)
		})
	}
}

func TestLocalRepository_GetReference(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := NewRepository(testRepo, config.Config)

	testcases := []struct {
		desc     string
		ref      string
		expected Reference
	}{
		{
			desc:     "fully qualified master branch",
			ref:      "refs/heads/master",
			expected: NewReference("refs/heads/master", masterOID.String()),
		},
		{
			desc:     "unqualified master branch fails",
			ref:      "master",
			expected: Reference{},
		},
		{
			desc:     "nonexistent branch",
			ref:      "refs/heads/nonexistent",
			expected: Reference{},
		},
		{
			desc:     "nonexistent branch",
			ref:      "nonexistent",
			expected: Reference{},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ref, err := repo.GetReference(ctx, ReferenceName(tc.ref))
			if tc.expected.Name == "" {
				require.True(t, errors.Is(err, ErrReferenceNotFound))
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, ref)
			}
		})
	}
}

func TestLocalRepository_GetReferences(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := NewRepository(testRepo, config.Config)

	testcases := []struct {
		desc    string
		pattern string
		match   func(t *testing.T, refs []Reference)
	}{
		{
			desc:    "master branch",
			pattern: "refs/heads/master",
			match: func(t *testing.T, refs []Reference) {
				require.Equal(t, []Reference{
					NewReference("refs/heads/master", masterOID.String()),
				}, refs)
			},
		},
		{
			desc:    "all references",
			pattern: "",
			match: func(t *testing.T, refs []Reference) {
				require.Len(t, refs, 94)
			},
		},
		{
			desc:    "branches",
			pattern: "refs/heads/",
			match: func(t *testing.T, refs []Reference) {
				require.Len(t, refs, 91)
			},
		},
		{
			desc:    "branches",
			pattern: "refs/heads/nonexistent",
			match: func(t *testing.T, refs []Reference) {
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

func TestLocalRepository_GetBranches(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := NewRepository(testRepo, config.Config)

	refs, err := repo.GetBranches(ctx)
	require.NoError(t, err)
	require.Len(t, refs, 91)
}

func TestLocalRepository_UpdateRef(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	defer func(oldValue string) {
		config.Config.Ruby.Dir = oldValue
	}(config.Config.Ruby.Dir)
	config.Config.Ruby.Dir = "/var/empty"

	otherRef, err := NewRepository(testRepo, config.Config).GetReference(ctx, "refs/heads/gitaly-test-ref")
	require.NoError(t, err)

	testcases := []struct {
		desc     string
		ref      string
		newValue ObjectID
		oldValue ObjectID
		verify   func(t *testing.T, repo *LocalRepository, err error)
	}{
		{
			desc:     "successfully update master",
			ref:      "refs/heads/master",
			newValue: ObjectID(otherRef.Target),
			oldValue: masterOID,
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, otherRef.Target)
			},
		},
		{
			desc:     "update fails with stale oldValue",
			ref:      "refs/heads/master",
			newValue: ObjectID(otherRef.Target),
			oldValue: nonexistentOID,
			verify: func(t *testing.T, repo *LocalRepository, err error) {
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
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, masterOID.String())
			},
		},
		{
			desc:     "successfully update master with empty oldValue",
			ref:      "refs/heads/master",
			newValue: ObjectID(otherRef.Target),
			oldValue: "",
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, otherRef.Target)
			},
		},
		{
			desc:     "updating unqualified branch fails",
			ref:      "master",
			newValue: ObjectID(otherRef.Target),
			oldValue: masterOID,
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, masterOID.String())
			},
		},
		{
			desc:     "deleting master succeeds",
			ref:      "refs/heads/master",
			newValue: ZeroOID,
			oldValue: masterOID,
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.NoError(t, err)
				_, err = repo.GetReference(ctx, "refs/heads/master")
				require.Error(t, err)
			},
		},
		{
			desc:     "creating new branch succeeds",
			ref:      "refs/heads/new",
			newValue: masterOID,
			oldValue: ZeroOID,
			verify: func(t *testing.T, repo *LocalRepository, err error) {
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

			repo := NewRepository(testRepo, config.Config)
			err := repo.UpdateRef(ctx, ReferenceName(tc.ref), tc.newValue, tc.oldValue)

			tc.verify(t, repo, err)
		})
	}
}
