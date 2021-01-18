package git

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// TestRepository tests an implementation of Repository.
func TestRepository(t *testing.T, getRepository func(testing.TB, *gitalypb.Repository) Repository) {
	for _, tc := range []struct {
		desc string
		test func(*testing.T, func(testing.TB, *gitalypb.Repository) Repository)
	}{
		{
			desc: "ResolveRevision",
			test: testRepositoryResolveRevision,
		},
		{
			desc: "HasBranches",
			test: testRepositoryHasBranches,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tc.test(t, getRepository)
		})
	}
}

func testRepositoryResolveRevision(t *testing.T, getRepository func(testing.TB, *gitalypb.Repository) Repository) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pbRepo, _, clean := testhelper.NewTestRepo(t)
	defer clean()

	for _, tc := range []struct {
		desc     string
		revision string
		expected ObjectID
	}{
		{
			desc:     "unqualified master branch",
			revision: "master",
			expected: "1e292f8fedd741b75372e19097c76d327140c312",
		},
		{
			desc:     "fully qualified master branch",
			revision: "refs/heads/master",
			expected: "1e292f8fedd741b75372e19097c76d327140c312",
		},
		{
			desc:     "typed commit",
			revision: "refs/heads/master^{commit}",
			expected: "1e292f8fedd741b75372e19097c76d327140c312",
		},
		{
			desc:     "extended SHA notation",
			revision: "refs/heads/master^2",
			expected: "c1c67abbaf91f624347bb3ae96eabe3a1b742478",
		},
		{
			desc:     "nonexistent branch",
			revision: "refs/heads/foobar",
		},
		{
			desc:     "SHA notation gone wrong",
			revision: "refs/heads/master^3",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			oid, err := getRepository(t, pbRepo).ResolveRevision(ctx, Revision(tc.revision))
			if tc.expected == "" {
				require.Equal(t, err, ErrReferenceNotFound)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, oid)
		})
	}
}

func testRepositoryHasBranches(t *testing.T, getRepository func(testing.TB, *gitalypb.Repository) Repository) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pbRepo, repoPath, clean := testhelper.InitBareRepo(t)
	defer clean()

	repo := getRepository(t, pbRepo)

	emptyCommit := text.ChompBytes(testhelper.MustRunCommand(t, nil,
		"git", "-C", repoPath, "commit-tree", EmptyTreeOID.String(),
	))

	testhelper.MustRunCommand(t, nil,
		"git", "-C", repoPath, "update-ref", "refs/headsbranch", emptyCommit,
	)

	hasBranches, err := repo.HasBranches(ctx)
	require.NoError(t, err)
	require.False(t, hasBranches)

	testhelper.MustRunCommand(t, nil,
		"git", "-C", repoPath, "update-ref", "refs/heads/branch", emptyCommit,
	)

	hasBranches, err = repo.HasBranches(ctx)
	require.NoError(t, err)
	require.True(t, hasBranches)
}
