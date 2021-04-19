// +build static,system_libgit2

package main

import (
	"errors"
	"testing"
	"time"

	git "github.com/libgit2/git2go/v31"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmdtesthelper "gitlab.com/gitlab-org/gitaly/cmd/gitaly-git2go/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
)

func TestRevert_validation(t *testing.T) {
	cfg, _, repoPath := testcfg.BuildWithRepo(t)
	testhelper.ConfigureGitalyGit2GoBin(t, cfg)

	testcases := []struct {
		desc        string
		request     git2go.RevertCommand
		expectedErr string
	}{
		{
			desc:        "no arguments",
			expectedErr: "revert: missing repository",
		},
		{
			desc:        "missing repository",
			request:     git2go.RevertCommand{AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD", Revert: "HEAD"},
			expectedErr: "revert: missing repository",
		},
		{
			desc:        "missing author name",
			request:     git2go.RevertCommand{Repository: repoPath, AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD", Revert: "HEAD"},
			expectedErr: "revert: missing author name",
		},
		{
			desc:        "missing author mail",
			request:     git2go.RevertCommand{Repository: repoPath, AuthorName: "Foo", Message: "Foo", Ours: "HEAD", Revert: "HEAD"},
			expectedErr: "revert: missing author mail",
		},
		{
			desc:        "missing message",
			request:     git2go.RevertCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Ours: "HEAD", Revert: "HEAD"},
			expectedErr: "revert: missing message",
		},
		{
			desc:        "missing ours",
			request:     git2go.RevertCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Revert: "HEAD"},
			expectedErr: "revert: missing ours",
		},
		{
			desc:        "missing revert",
			request:     git2go.RevertCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD"},
			expectedErr: "revert: missing revert",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := tc.request.Run(ctx, cfg)
			require.Error(t, err)
			require.EqualError(t, err, tc.expectedErr)
		})
	}
}

func TestRevert_trees(t *testing.T) {
	testcases := []struct {
		desc             string
		setupRepo        func(t testing.TB, repoPath string) (ours, revert string)
		expected         map[string]string
		expectedCommitID string
		expectedErr      string
		expectedErrAs    interface{}
	}{
		{
			desc: "trivial revert succeeds",
			setupRepo: func(t testing.TB, repoPath string) (ours, revert string) {
				baseOid := cmdtesthelper.BuildCommit(t, repoPath, nil, map[string]string{
					"a": "apple",
					"b": "banana",
				})
				revertOid := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{baseOid}, map[string]string{
					"a": "apple",
					"b": "pineapple",
				})
				oursOid := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{revertOid}, map[string]string{
					"a": "apple",
					"b": "pineapple",
					"c": "carrot",
				})

				return oursOid.String(), revertOid.String()
			},
			expected: map[string]string{
				"a": "apple",
				"b": "banana",
				"c": "carrot",
			},
			expectedCommitID: "0be709b57f18ad3f592a8cb7c57f920861392573",
		},
		{
			desc: "conflicting revert fails",
			setupRepo: func(t testing.TB, repoPath string) (ours, revert string) {
				baseOid := cmdtesthelper.BuildCommit(t, repoPath, nil, map[string]string{
					"a": "apple",
				})
				revertOid := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{baseOid}, map[string]string{
					"a": "pineapple",
				})
				oursOid := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{revertOid}, map[string]string{
					"a": "carrot",
				})

				return oursOid.String(), revertOid.String()
			},
			expectedErr:   "revert: could not apply due to conflicts",
			expectedErrAs: &git2go.HasConflictsError{},
		},
		{
			desc: "empty revert fails",
			setupRepo: func(t testing.TB, repoPath string) (ours, revert string) {
				baseOid := cmdtesthelper.BuildCommit(t, repoPath, nil, map[string]string{
					"a": "apple",
				})
				revertOid := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{baseOid}, map[string]string{
					"a": "banana",
				})
				oursOid := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{revertOid}, map[string]string{
					"a": "apple",
				})

				return oursOid.String(), revertOid.String()
			},
			expectedErr:   "revert: could not apply because the result was empty",
			expectedErrAs: &git2go.EmptyError{},
		},
		{
			desc: "nonexistent ours fails",
			setupRepo: func(t testing.TB, repoPath string) (ours, revert string) {
				revertOid := cmdtesthelper.BuildCommit(t, repoPath, nil, map[string]string{
					"a": "apple",
				})

				return "nonexistent", revertOid.String()
			},
			expectedErr: "revert: ours commit lookup: lookup commit \"nonexistent\": revspec 'nonexistent' not found",
		},
		{
			desc: "nonexistent revert fails",
			setupRepo: func(t testing.TB, repoPath string) (ours, revert string) {
				oursOid := cmdtesthelper.BuildCommit(t, repoPath, nil, map[string]string{
					"a": "apple",
				})

				return oursOid.String(), "nonexistent"
			},
			expectedErr: "revert: revert commit lookup: lookup commit \"nonexistent\": revspec 'nonexistent' not found",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, _, repoPath := testcfg.BuildWithRepo(t)
			testhelper.ConfigureGitalyGit2GoBin(t, cfg)

			ours, revert := tc.setupRepo(t, repoPath)

			ctx, cancel := testhelper.Context()
			defer cancel()

			authorDate := time.Date(2020, 7, 30, 7, 45, 50, 0, time.FixedZone("UTC+2", +2*60*60))

			request := git2go.RevertCommand{
				Repository: repoPath,
				AuthorName: "Foo",
				AuthorMail: "foo@example.com",
				AuthorDate: authorDate,
				Message:    "Foo",
				Ours:       ours,
				Revert:     revert,
			}

			response, err := request.Run(ctx, cfg)

			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)

				if tc.expectedErrAs != nil {
					require.True(t, errors.As(err, tc.expectedErrAs))
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedCommitID, response.String())

			repo, err := git.OpenRepository(repoPath)
			require.NoError(t, err)
			defer repo.Free()

			commitOid, err := git.NewOid(response.String())
			require.NoError(t, err)

			commit, err := repo.LookupCommit(commitOid)
			require.NoError(t, err)

			tree, err := commit.Tree()
			require.NoError(t, err)
			require.EqualValues(t, len(tc.expected), tree.EntryCount())

			for name, contents := range tc.expected {
				entry := tree.EntryByName(name)
				require.NotNil(t, entry)

				blob, err := repo.LookupBlob(entry.Id)
				require.NoError(t, err)
				require.Equal(t, []byte(contents), blob.Contents())
			}
		})
	}
}
