package gittest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

// GetRepositoryFunc is used to get a clean test repository for the different implementations of the
// Repository interface in the common test suite TestRepository.
type GetRepositoryFunc func(ctx context.Context, t testing.TB, seeded bool) (git.Repository, string)

// TestRepository tests an implementation of Repository.
func TestRepository(t *testing.T, cfg config.Cfg, getRepository GetRepositoryFunc) {
	for _, tc := range []struct {
		desc string
		test func(*testing.T, config.Cfg, GetRepositoryFunc)
	}{
		{
			desc: "ResolveRevision",
			test: testRepositoryResolveRevision,
		},
		{
			desc: "HasBranches",
			test: testRepositoryHasBranches,
		},
		{
			desc: "GetDefaultBranch",
			test: testRepositoryGetDefaultBranch,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tc.test(t, cfg, getRepository)
		})
	}
}

func testRepositoryResolveRevision(t *testing.T, cfg config.Cfg, getRepository GetRepositoryFunc) {
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc     string
		revision string
		expected git.ObjectID
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
			repo, _ := getRepository(ctx, t, true)
			oid, err := repo.ResolveRevision(ctx, git.Revision(tc.revision))
			if tc.expected == "" {
				require.Equal(t, err, git.ErrReferenceNotFound)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, oid)
		})
	}
}

func testRepositoryHasBranches(t *testing.T, cfg config.Cfg, getRepository GetRepositoryFunc) {
	ctx := testhelper.Context(t)

	repo, repoPath := getRepository(ctx, t, false)

	emptyCommit := text.ChompBytes(Exec(t, cfg, "-C", repoPath, "commit-tree", git.EmptyTreeOID.String()))

	Exec(t, cfg, "-C", repoPath, "update-ref", "refs/headsbranch", emptyCommit)

	hasBranches, err := repo.HasBranches(ctx)
	require.NoError(t, err)
	require.False(t, hasBranches)

	Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/branch", emptyCommit)

	hasBranches, err = repo.HasBranches(ctx)
	require.NoError(t, err)
	require.True(t, hasBranches)
}

func testRepositoryGetDefaultBranch(t *testing.T, cfg config.Cfg, getRepository GetRepositoryFunc) {
	const testOID = "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc         string
		repo         func(t *testing.T) git.Repository
		expectedName git.ReferenceName
	}{
		{
			desc: "default ref",
			repo: func(t *testing.T) git.Repository {
				repo, repoPath := getRepository(ctx, t, false)
				oid := WriteCommit(t, cfg, repoPath, WithParents(), WithBranch("apple"))
				WriteCommit(t, cfg, repoPath, WithParents(oid), WithBranch("main"))
				return repo
			},
			expectedName: git.DefaultRef,
		},
		{
			desc: "legacy default ref",
			repo: func(t *testing.T) git.Repository {
				repo, repoPath := getRepository(ctx, t, false)
				oid := WriteCommit(t, cfg, repoPath, WithParents(), WithBranch("apple"))
				WriteCommit(t, cfg, repoPath, WithParents(oid), WithBranch("master"))
				return repo
			},
			expectedName: git.LegacyDefaultRef,
		},
		{
			desc: "no branches",
			repo: func(t *testing.T) git.Repository {
				repo, _ := getRepository(ctx, t, false)
				return repo
			},
		},
		{
			desc: "one branch",
			repo: func(t *testing.T) git.Repository {
				repo, repoPath := getRepository(ctx, t, false)
				WriteCommit(t, cfg, repoPath, WithParents(), WithBranch("apple"))
				return repo
			},
			expectedName: git.NewReferenceNameFromBranchName("apple"),
		},
		{
			desc: "no default branches",
			repo: func(t *testing.T) git.Repository {
				repo, repoPath := getRepository(ctx, t, false)
				oid := WriteCommit(t, cfg, repoPath, WithParents(), WithBranch("apple"))
				WriteCommit(t, cfg, repoPath, WithParents(oid), WithBranch("banana"))
				return repo
			},
			expectedName: git.NewReferenceNameFromBranchName("apple"),
		},
		{
			desc: "test repo default",
			repo: func(t *testing.T) git.Repository {
				repo, _ := getRepository(ctx, t, true)
				return repo
			},
			expectedName: git.LegacyDefaultRef,
		},
		{
			desc: "test repo HEAD set",
			repo: func(t *testing.T) git.Repository {
				repo, repoPath := getRepository(ctx, t, true)
				Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/feature", testOID)
				Exec(t, cfg, "-C", repoPath, "symbolic-ref", "HEAD", "refs/heads/feature")
				return repo
			},
			expectedName: git.NewReferenceNameFromBranchName("feature"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			name, err := tc.repo(t).GetDefaultBranch(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.expectedName, name)
		})
	}
}
