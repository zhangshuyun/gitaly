package localrepo

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	masterOID      = git.ObjectID("1e292f8fedd741b75372e19097c76d327140c312")
	nonexistentOID = git.ObjectID("ba4f184e126b751d1bffad5897f263108befc780")
)

func TestRepo_ContainsRef(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	repo, _ := setupRepo(t, false)

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

	repo, _ := setupRepo(t, false)

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

	repo, _ := setupRepo(t, false)

	// Disable hooks
	repo.cfg.Ruby.Dir = "/var/empty"

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

	repo, _ := setupRepo(t, false)

	masterBranch, err := repo.GetReference(ctx, "refs/heads/master")
	require.NoError(t, err)

	testcases := []struct {
		desc     string
		patterns []string
		match    func(t *testing.T, refs []git.Reference)
	}{
		{
			desc:     "master branch",
			patterns: []string{"refs/heads/master"},
			match: func(t *testing.T, refs []git.Reference) {
				require.Equal(t, []git.Reference{masterBranch}, refs)
			},
		},
		{
			desc:     "two branches",
			patterns: []string{"refs/heads/master", "refs/heads/feature"},
			match: func(t *testing.T, refs []git.Reference) {
				featureBranch, err := repo.GetReference(ctx, "refs/heads/feature")
				require.NoError(t, err)

				require.Equal(t, []git.Reference{featureBranch, masterBranch}, refs)
			},
		},
		{
			desc:     "matching subset is returned",
			patterns: []string{"refs/heads/master", "refs/heads/nonexistent"},
			match: func(t *testing.T, refs []git.Reference) {
				require.Equal(t, []git.Reference{masterBranch}, refs)
			},
		},
		{
			desc: "all references",
			match: func(t *testing.T, refs []git.Reference) {
				require.Len(t, refs, 96)
			},
		},
		{
			desc:     "branches",
			patterns: []string{"refs/heads/"},
			match: func(t *testing.T, refs []git.Reference) {
				require.Len(t, refs, 93)
			},
		},
		{
			desc:     "non-existent branch",
			patterns: []string{"refs/heads/nonexistent"},
			match: func(t *testing.T, refs []git.Reference) {
				require.Empty(t, refs)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			refs, err := repo.GetReferences(ctx, tc.patterns...)
			require.NoError(t, err)
			tc.match(t, refs)
		})
	}
}

func TestRepo_GetRemoteReferences(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg := testcfg.Build(t)

	storagePath, ok := cfg.StoragePath("default")
	require.True(t, ok)

	const relativePath = "repository-1"
	repoPath := filepath.Join(storagePath, relativePath)

	gittest.Exec(t, cfg, "init", repoPath)
	gittest.Exec(t, cfg, "-C", repoPath, "commit", "--allow-empty", "-m", "commit message")
	commit := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/master"))

	for _, cmd := range [][]string{
		{"update-ref", "refs/heads/master", commit},
		{"tag", "lightweight-tag", commit},
		{"tag", "-m", "tag message", "annotated-tag", "refs/heads/master"},
		{"symbolic-ref", "refs/heads/symbolic", "refs/heads/master"},
		{"update-ref", "refs/remote/remote-name/remote-branch", commit},
	} {
		gittest.Exec(t, cfg, append([]string{"-C", repoPath}, cmd...)...)
	}

	annotatedTagOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "annotated-tag"))

	gitCmdFactory := git.NewExecCommandFactory(cfg)
	repo := New(
		gitCmdFactory,
		catfile.NewCache(cfg),
		&gitalypb.Repository{StorageName: "default", RelativePath: filepath.Join(relativePath, ".git")},
		cfg,
	)
	for _, tc := range []struct {
		desc     string
		patterns []string
		expected []git.Reference
	}{
		{
			desc:     "not found",
			patterns: []string{"this-pattern-does-not-match-anything"},
		},
		{
			desc: "all",
			expected: []git.Reference{
				{Name: "refs/heads/master", Target: commit},
				{Name: "refs/heads/symbolic", Target: commit},
				{Name: "refs/remote/remote-name/remote-branch", Target: commit},
				{Name: "refs/tags/annotated-tag", Target: annotatedTagOID},
				{Name: "refs/tags/lightweight-tag", Target: commit},
			},
		},
		{
			desc:     "branches and tags only",
			patterns: []string{"refs/heads/*", "refs/tags/*"},
			expected: []git.Reference{
				{Name: "refs/heads/master", Target: commit},
				{Name: "refs/heads/symbolic", Target: commit},
				{Name: "refs/tags/annotated-tag", Target: annotatedTagOID},
				{Name: "refs/tags/lightweight-tag", Target: commit},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			refs, err := repo.GetRemoteReferences(ctx, repoPath, tc.patterns...)
			require.NoError(t, err)
			require.Equal(t, tc.expected, refs)
		})
	}
}

func TestRepo_GetBranches(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	repo, _ := setupRepo(t, false)

	refs, err := repo.GetBranches(ctx)
	require.NoError(t, err)
	require.Len(t, refs, 93)
}

func TestRepo_UpdateRef(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	repo, _ := setupRepo(t, false)

	// Disable hooks
	repo.cfg.Ruby.Dir = "/var/empty"

	otherRef, err := repo.GetReference(ctx, "refs/heads/gitaly-test-ref")
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
			repoProto, _, _ := gittest.CloneRepoAtStorage(t, repo.cfg, repo.cfg.Storages[0], t.Name())
			repo := New(repo.gitCmdFactory, repo.catfileCache, repoProto, repo.cfg)
			err := repo.UpdateRef(ctx, git.ReferenceName(tc.ref), tc.newValue, tc.oldValue)
			tc.verify(t, repo, err)
		})
	}
}
