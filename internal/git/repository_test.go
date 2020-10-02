package git

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

const (
	MasterID      = "1e292f8fedd741b75372e19097c76d327140c312"
	NonexistentID = "ba4f184e126b751d1bffad5897f263108befc780"
)

func TestLocalRepository_ResolveRefish(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	repo := NewRepository(testhelper.TestRepository())

	testcases := []struct {
		desc     string
		refish   string
		expected string
	}{
		{
			desc:     "unqualified master branch",
			refish:   "master",
			expected: MasterID,
		},
		{
			desc:     "fully qualified master branch",
			refish:   "refs/heads/master",
			expected: MasterID,
		},
		{
			desc:     "typed commit",
			refish:   "refs/heads/master^{commit}",
			expected: MasterID,
		},
		{
			desc:     "extended SHA notation",
			refish:   "refs/heads/master^2",
			expected: "c1c67abbaf91f624347bb3ae96eabe3a1b742478",
		},
		{
			desc:   "nonexistent branch",
			refish: "refs/heads/foobar",
		},
		{
			desc:   "SHA notation gone wrong",
			refish: "refs/heads/master^3",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			oid, err := repo.ResolveRefish(ctx, tc.refish)

			if tc.expected == "" {
				require.Error(t, err)
				require.Equal(t, err, ErrReferenceNotFound)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, oid)
		})
	}
}

func TestLocalRepository_ContainsRef(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	repo := NewRepository(testhelper.TestRepository())

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
			contained, err := repo.ContainsRef(ctx, tc.ref)
			require.NoError(t, err)
			require.Equal(t, tc.contained, contained)
		})
	}
}

func TestLocalRepository_GetReference(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	repo := NewRepository(testhelper.TestRepository())

	testcases := []struct {
		desc     string
		ref      string
		expected Reference
	}{
		{
			desc:     "fully qualified master branch",
			ref:      "refs/heads/master",
			expected: NewReference("refs/heads/master", MasterID),
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
			ref, err := repo.GetReference(ctx, tc.ref)
			if tc.expected.Name == "" {
				require.True(t, errors.Is(err, ErrReferenceNotFound))
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, ref)
			}
		})
	}
}

func TestLocalRepository_GetBranch(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	repo := NewRepository(testhelper.TestRepository())

	testcases := []struct {
		desc     string
		ref      string
		expected Reference
	}{
		{
			desc:     "fully qualified master branch",
			ref:      "refs/heads/master",
			expected: NewReference("refs/heads/master", MasterID),
		},
		{
			desc:     "half-qualified master branch",
			ref:      "heads/master",
			expected: NewReference("refs/heads/master", MasterID),
		},
		{
			desc:     "fully qualified master branch",
			ref:      "master",
			expected: NewReference("refs/heads/master", MasterID),
		},
		{
			desc:     "nonexistent branch",
			ref:      "nonexistent",
			expected: Reference{},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ref, err := repo.GetBranch(ctx, tc.ref)
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

	repo := NewRepository(testhelper.TestRepository())

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
					NewReference("refs/heads/master", MasterID),
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

	repo := NewRepository(testhelper.TestRepository())

	refs, err := repo.GetBranches(ctx)
	require.NoError(t, err)
	require.Len(t, refs, 91)
}

func TestLocalRepository_UpdateRef(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	otherRef, err := NewRepository(testRepo).GetReference(ctx, "refs/heads/gitaly-test-ref")
	require.NoError(t, err)

	testcases := []struct {
		desc   string
		ref    string
		newrev string
		oldrev string
		verify func(t *testing.T, repo Repository, err error)
	}{
		{
			desc:   "successfully update master",
			ref:    "refs/heads/master",
			newrev: otherRef.Target,
			oldrev: MasterID,
			verify: func(t *testing.T, repo Repository, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, otherRef.Target)
			},
		},
		{
			desc:   "update fails with stale oldrev",
			ref:    "refs/heads/master",
			newrev: otherRef.Target,
			oldrev: NonexistentID,
			verify: func(t *testing.T, repo Repository, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, MasterID)
			},
		},
		{
			desc:   "update fails with invalid newrev",
			ref:    "refs/heads/master",
			newrev: NonexistentID,
			oldrev: MasterID,
			verify: func(t *testing.T, repo Repository, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, MasterID)
			},
		},
		{
			desc:   "successfully update master with empty oldrev",
			ref:    "refs/heads/master",
			newrev: otherRef.Target,
			oldrev: "",
			verify: func(t *testing.T, repo Repository, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, otherRef.Target)
			},
		},
		{
			desc:   "updating unqualified branch fails",
			ref:    "master",
			newrev: otherRef.Target,
			oldrev: MasterID,
			verify: func(t *testing.T, repo Repository, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, MasterID)
			},
		},
		{
			desc:   "deleting master succeeds",
			ref:    "refs/heads/master",
			newrev: strings.Repeat("0", 40),
			oldrev: MasterID,
			verify: func(t *testing.T, repo Repository, err error) {
				require.NoError(t, err)
				_, err = repo.GetReference(ctx, "refs/heads/master")
				require.Error(t, err)
			},
		},
		{
			desc:   "creating new branch succeeds",
			ref:    "refs/heads/new",
			newrev: MasterID,
			oldrev: strings.Repeat("0", 40),
			verify: func(t *testing.T, repo Repository, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/new")
				require.NoError(t, err)
				require.Equal(t, ref.Target, MasterID)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			// Re-create repo for each testcase.
			testRepo, _, cleanup := testhelper.NewTestRepo(t)
			defer cleanup()

			repo := NewRepository(testRepo)
			err := repo.UpdateRef(ctx, tc.ref, tc.newrev, tc.oldrev)

			tc.verify(t, repo, err)
		})
	}
}
