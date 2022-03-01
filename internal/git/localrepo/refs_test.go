package localrepo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/peer"
)

const (
	masterOID      = git.ObjectID("1e292f8fedd741b75372e19097c76d327140c312")
	nonexistentOID = git.ObjectID("ba4f184e126b751d1bffad5897f263108befc780")
)

func TestRepo_ContainsRef(t *testing.T) {
	ctx := testhelper.Context(t)

	_, repo, _ := setupRepo(t)

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
	ctx := testhelper.Context(t)

	_, repo, _ := setupRepo(t)

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
	ctx := testhelper.Context(t)

	_, repo, _ := setupRepo(t, withDisabledHooks())

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
	ctx := testhelper.Context(t)

	_, repo, _ := setupRepo(t)

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
				require.Len(t, refs, 97)
			},
		},
		{
			desc:     "branches",
			patterns: []string{"refs/heads/"},
			match: func(t *testing.T, refs []git.Reference) {
				require.Len(t, refs, 94)
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
	ctx := testhelper.Context(t)

	cfg := testcfg.Build(t)

	gitCmdFactory, readSSHCommand := captureGitSSHCommand(ctx, t, cfg)

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

	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()

	repo := New(
		config.NewLocator(cfg),
		gitCmdFactory,
		catfileCache,
		&gitalypb.Repository{StorageName: "default", RelativePath: filepath.Join(relativePath, ".git")},
	)
	for _, tc := range []struct {
		desc                  string
		remote                string
		opts                  []GetRemoteReferencesOption
		expected              []git.Reference
		expectedGitSSHCommand string
	}{
		{
			desc:   "not found",
			remote: repoPath,
			opts: []GetRemoteReferencesOption{
				WithPatterns("this-pattern-does-not-match-anything"),
			},
		},
		{
			desc:   "all",
			remote: repoPath,
			expected: []git.Reference{
				{Name: "refs/heads/master", Target: commit},
				{Name: "refs/heads/symbolic", Target: commit, IsSymbolic: true},
				{Name: "refs/remote/remote-name/remote-branch", Target: commit},
				{Name: "refs/tags/annotated-tag", Target: annotatedTagOID},
				{Name: "refs/tags/lightweight-tag", Target: commit},
			},
		},
		{
			desc:   "branches and tags only",
			remote: repoPath,
			opts: []GetRemoteReferencesOption{
				WithPatterns("refs/heads/*", "refs/tags/*"),
			},
			expected: []git.Reference{
				{Name: "refs/heads/master", Target: commit},
				{Name: "refs/heads/symbolic", Target: commit, IsSymbolic: true},
				{Name: "refs/tags/annotated-tag", Target: annotatedTagOID},
				{Name: "refs/tags/lightweight-tag", Target: commit},
			},
		},
		{
			desc:   "with in-memory remote",
			remote: "inmemory",
			opts: []GetRemoteReferencesOption{
				WithPatterns("refs/heads/master"),
				WithConfig(git.ConfigPair{
					Key:   "remote.inmemory.url",
					Value: repoPath,
				}),
			},
			expected: []git.Reference{
				{Name: "refs/heads/master", Target: commit},
			},
		},
		{
			desc:   "with custom ssh command",
			remote: repoPath,
			opts: []GetRemoteReferencesOption{
				WithPatterns("refs/heads/master"),
				WithSSHCommand("custom-ssh -with-creds"),
			},
			expected: []git.Reference{
				{Name: "refs/heads/master", Target: commit},
			},
			expectedGitSSHCommand: "custom-ssh -with-creds",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			refs, err := repo.GetRemoteReferences(ctx, tc.remote, tc.opts...)
			require.NoError(t, err)
			require.Equal(t, tc.expected, refs)

			gitSSHCommand, err := readSSHCommand()
			if !os.IsNotExist(err) {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedGitSSHCommand, string(gitSSHCommand))
		})
	}
}

func TestRepo_GetBranches(t *testing.T) {
	ctx := testhelper.Context(t)

	_, repo, _ := setupRepo(t)

	refs, err := repo.GetBranches(ctx)
	require.NoError(t, err)
	require.Len(t, refs, 94)
}

func TestRepo_UpdateRef(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, repo, _ := setupRepo(t, withDisabledHooks())

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
			repoProto, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
			repo := New(repo.locator, repo.gitCmdFactory, repo.catfileCache, repoProto)
			err := repo.UpdateRef(ctx, git.ReferenceName(tc.ref), tc.newValue, tc.oldValue)
			tc.verify(t, repo, err)
		})
	}
}

func TestRepo_SetDefaultBranch(t *testing.T) {
	testhelper.NewFeatureSets(featureflag.TransactionalSymbolicRefUpdates).Run(t, testRepoSetBranchFeatures)
}

func testRepoSetBranchFeatures(t *testing.T, ctx context.Context) {
	_, repo, _ := setupRepo(t)

	txManager := transaction.NewTrackingManager()

	testCases := []struct {
		desc        string
		ref         git.ReferenceName
		expectedRef git.ReferenceName
	}{
		{
			desc:        "update the branch ref",
			ref:         "refs/heads/feature",
			expectedRef: "refs/heads/feature",
		},
		{
			desc:        "unknown ref",
			ref:         "refs/heads/non_existent_ref",
			expectedRef: git.LegacyDefaultRef,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			txManager.Reset()
			ctx, err := txinfo.InjectTransaction(
				peer.NewContext(ctx, &peer.Peer{}),
				1,
				"node",
				true,
			)
			require.NoError(t, err)

			require.NoError(t, repo.SetDefaultBranch(ctx, txManager, tc.ref))

			newRef, err := repo.GetDefaultBranch(ctx)
			require.NoError(t, err)

			require.Equal(t, tc.expectedRef, newRef)

			if featureflag.TransactionalSymbolicRefUpdates.IsEnabled(ctx) {
				require.Len(t, txManager.Votes(), 2)
				h := voting.NewVoteHash()
				_, err = h.Write([]byte("ref: " + tc.ref.String() + "\n"))
				require.NoError(t, err)
				vote, err := h.Vote()
				require.NoError(t, err)

				require.Equal(t, voting.Prepared, txManager.Votes()[0].Phase)
				require.Equal(t, vote.String(), txManager.Votes()[0].Vote.String())
				require.Equal(t, voting.Committed, txManager.Votes()[1].Phase)
				require.Equal(t, vote.String(), txManager.Votes()[1].Vote.String())
			}
		})
	}
}

type blockingManager struct {
	ch chan struct{}
}

func (b *blockingManager) Vote(_ context.Context, _ txinfo.Transaction, _ voting.Vote, phase voting.Phase) error {
	// the purpose of this is to block SetDefaultBranch from completing, so just choose to block on
	// a Prepared vote.
	if phase == voting.Prepared {
		b.ch <- struct{}{}
		<-b.ch
	}

	return nil
}

func (b *blockingManager) Stop(_ context.Context, _ txinfo.Transaction) error {
	return nil
}

func TestRepo_SetDefaultBranch_errors(t *testing.T) {
	ctx := featureflag.ContextWithFeatureFlag(
		testhelper.Context(t),
		featureflag.TransactionalSymbolicRefUpdates,
		true)

	t.Run("malformed refname", func(t *testing.T) {
		_, repo, _ := setupRepo(t)

		err := repo.SetDefaultBranch(ctx, &transaction.MockManager{}, "./.lock")
		require.EqualError(t, err, `"./.lock" is a malformed refname`)
	})

	t.Run("HEAD is locked by another process", func(t *testing.T) {
		_, repo, _ := setupRepo(t)

		ref, err := repo.GetDefaultBranch(ctx)
		require.NoError(t, err)

		path, err := repo.Path()
		require.NoError(t, err)

		require.NoError(t, os.WriteFile(filepath.Join(path, "HEAD.lock"), []byte(""), 0o644))

		err = repo.SetDefaultBranch(ctx, &transaction.MockManager{}, "refs/heads/branch")
		require.ErrorIs(t, err, safe.ErrFileAlreadyLocked)

		refAfter, err := repo.GetDefaultBranch(ctx)
		require.NoError(t, err)
		require.Equal(t, ref, refAfter)
	})

	t.Run("HEAD is locked by SetDefaultBranch", func(t *testing.T) {
		ctx, err := txinfo.InjectTransaction(
			peer.NewContext(ctx, &peer.Peer{}),
			1,
			"node",
			true,
		)

		require.NoError(t, err)

		_, repo, _ := setupRepo(t)

		ch := make(chan struct{})
		doneCh := make(chan struct{})
		go func() {
			_ = repo.SetDefaultBranch(ctx, &blockingManager{ch}, "refs/heads/branch")
			doneCh <- struct{}{}
		}()
		<-ch

		var stderr bytes.Buffer
		err = repo.ExecAndWait(ctx, git.SubCmd{
			Name: "symbolic-ref",
			Args: []string{"HEAD", "refs/heads/otherbranch"},
		}, git.WithRefTxHook(repo), git.WithStderr(&stderr))

		code, ok := command.ExitStatus(err)
		require.True(t, ok)
		assert.Equal(t, 1, code)
		assert.Regexp(t, "Unable to create .+\\/HEAD\\.lock': File exists.", stderr.String())
		ch <- struct{}{}
		<-doneCh
	})

	t.Run("failing vote unlocks symref", func(t *testing.T) {
		ctx, err := txinfo.InjectTransaction(
			peer.NewContext(ctx, &peer.Peer{}),
			1,
			"node",
			true,
		)
		require.NoError(t, err)

		_, repo, repoPath := setupRepo(t)

		failingTxManager := &transaction.MockManager{
			VoteFn: func(context.Context, txinfo.Transaction, voting.Vote, voting.Phase) error {
				return errors.New("injected error")
			},
		}

		err = repo.SetDefaultBranch(ctx, failingTxManager, "refs/heads/branch")
		require.Error(t, err)
		require.Equal(t, "committing temporary HEAD: voting on locked file: preimage vote: injected error", err.Error())
		require.NoFileExists(t, filepath.Join(repoPath, "HEAD.lock"))
	})
}

func TestGuessHead(t *testing.T) {
	cfg, repo, repoPath := setupRepo(t)

	commit1 := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/master"))
	commit2 := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/feature"))

	for _, tc := range []struct {
		desc        string
		cmds        [][]string
		head        git.Reference
		expected    git.ReferenceName
		expectedErr error
	}{
		{
			desc:     "symbolic",
			head:     git.NewSymbolicReference("HEAD", "refs/heads/something"),
			expected: "refs/heads/something",
		},
		{
			desc: "matching default branch",
			cmds: [][]string{
				{"update-ref", git.DefaultRef.String(), commit1},
				{"update-ref", git.LegacyDefaultRef.String(), commit2},
				{"update-ref", "refs/heads/apple", commit1},
				{"update-ref", "refs/heads/feature", commit1},
				{"update-ref", "refs/heads/zucchini", commit1},
			},
			head:     git.NewReference("HEAD", commit1),
			expected: git.DefaultRef,
		},
		{
			desc: "matching default legacy branch",
			cmds: [][]string{
				{"update-ref", git.DefaultRef.String(), commit2},
				{"update-ref", git.LegacyDefaultRef.String(), commit1},
				{"update-ref", "refs/heads/apple", commit1},
				{"update-ref", "refs/heads/feature", commit1},
				{"update-ref", "refs/heads/zucchini", commit1},
			},
			head:     git.NewReference("HEAD", commit1),
			expected: git.LegacyDefaultRef,
		},
		{
			desc: "matching other branch",
			cmds: [][]string{
				{"update-ref", git.DefaultRef.String(), commit2},
				{"update-ref", git.LegacyDefaultRef.String(), commit2},
				{"update-ref", "refs/heads/apple", commit1},
				{"update-ref", "refs/heads/feature", commit1},
				{"update-ref", "refs/heads/zucchini", commit1},
			},
			head:     git.NewReference("HEAD", commit1),
			expected: "refs/heads/apple",
		},
		{
			desc: "missing default branches",
			cmds: [][]string{
				{"update-ref", "-d", git.DefaultRef.String()},
				{"update-ref", "-d", git.LegacyDefaultRef.String()},
				{"update-ref", "refs/heads/apple", commit1},
				{"update-ref", "refs/heads/feature", commit1},
				{"update-ref", "refs/heads/zucchini", commit1},
			},
			head:     git.NewReference("HEAD", commit1),
			expected: "refs/heads/apple",
		},
		{
			desc: "no match",
			cmds: [][]string{
				{"update-ref", git.DefaultRef.String(), commit2},
				{"update-ref", git.LegacyDefaultRef.String(), commit2},
				{"update-ref", "refs/heads/apple", commit2},
				{"update-ref", "refs/heads/feature", commit2},
				{"update-ref", "refs/heads/zucchini", commit2},
			},
			head:        git.NewReference("HEAD", commit1),
			expectedErr: fmt.Errorf("guess head: %w", git.ErrReferenceNotFound),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := testhelper.Context(t)

			for _, cmd := range tc.cmds {
				gittest.Exec(t, cfg, append([]string{"-C", repoPath}, cmd...)...)
			}

			guess, err := repo.GuessHead(ctx, tc.head)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.expectedErr, err)
			}
			require.Equal(t, tc.expected, guess)
		})
	}
}
