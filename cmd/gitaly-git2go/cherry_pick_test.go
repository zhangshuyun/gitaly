// +build static,system_libgit2

package main

import (
	"errors"
	"testing"
	"time"

	git "github.com/libgit2/git2go/v30"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmdtesthelper "gitlab.com/gitlab-org/gitaly/cmd/gitaly-git2go/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestCherryPick_validation(t *testing.T) {
	_, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testcases := []struct {
		desc        string
		request     git2go.CherryPickCommand
		expectedErr string
	}{
		{
			desc:        "no arguments",
			expectedErr: "cherry-pick: missing repository",
		},
		{
			desc:        "missing repository",
			request:     git2go.CherryPickCommand{AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing repository",
		},
		{
			desc:        "missing author name",
			request:     git2go.CherryPickCommand{Repository: repoPath, AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing author name",
		},
		{
			desc:        "missing author mail",
			request:     git2go.CherryPickCommand{Repository: repoPath, AuthorName: "Foo", Message: "Foo", Ours: "HEAD", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing author mail",
		},
		{
			desc:        "missing message",
			request:     git2go.CherryPickCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Ours: "HEAD", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing message",
		},
		{
			desc:        "missing ours",
			request:     git2go.CherryPickCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing ours",
		},
		{
			desc:        "missing commit",
			request:     git2go.CherryPickCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD"},
			expectedErr: "cherry-pick: missing commit",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := tc.request.Run(ctx, config.Config)
			require.EqualError(t, err, tc.expectedErr)
		})
	}
}

func TestCherryPick(t *testing.T) {
	testcases := []struct {
		desc             string
		base             map[string]string
		ours             map[string]string
		commit           map[string]string
		expected         map[string]string
		expectedCommitID string
		expectedErr      error
		expectedErrMsg   string
	}{
		{
			desc: "trivial cherry-pick succeeds",
			base: map[string]string{
				"file": "foo",
			},
			ours: map[string]string{
				"file": "foo",
			},
			commit: map[string]string{
				"file": "foobar",
			},
			expected: map[string]string{
				"file": "foobar",
			},
			expectedCommitID: "a6b964c97f96f6e479f602633a43bc83c84e6688",
		},
		{
			desc: "conflicting cherry-pick fails",
			base: map[string]string{
				"file": "foo",
			},
			ours: map[string]string{
				"file": "fooqux",
			},
			commit: map[string]string{
				"file": "foobar",
			},
			expectedErr:    git2go.HasConflictsError{},
			expectedErrMsg: "cherry-pick: could not apply due to conflicts",
		},
		{
			desc:           "fails on nonexistent ours commit",
			expectedErrMsg: "cherry-pick: ours commit lookup: could not lookup reference \"nonexistent\": revspec 'nonexistent' not found",
		},
		{
			desc: "fails on nonexistent cherry-pick commit",
			ours: map[string]string{
				"file": "fooqux",
			},
			expectedErrMsg: "cherry-pick: commit lookup: could not lookup reference \"nonexistent\": revspec 'nonexistent' not found",
		},
	}
	for _, tc := range testcases {
		_, repoPath, cleanup := testhelper.NewTestRepo(t)
		defer cleanup()

		base := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{nil}, tc.base)

		var ours, commit = "nonexistent", "nonexistent"
		if len(tc.ours) > 0 {
			ours = cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, tc.ours).String()
		}
		if len(tc.commit) > 0 {
			commit = cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, tc.commit).String()
		}

		authorDate := time.Date(2021, 1, 17, 14, 45, 51, 0, time.FixedZone("UTC+2", +2*60*60))

		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := git2go.CherryPickCommand{
				Repository: repoPath,
				AuthorName: "Foo",
				AuthorMail: "foo@example.com",
				AuthorDate: authorDate,
				Message:    "Foo",
				Ours:       ours,
				Commit:     commit,
			}.Run(ctx, config.Config)

			if tc.expectedErrMsg != "" {
				require.EqualError(t, err, tc.expectedErrMsg)

				if tc.expectedErr != nil {
					require.True(t, errors.Is(err, tc.expectedErr))
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedCommitID, response)

			repo, err := git.OpenRepository(repoPath)
			require.NoError(t, err)
			defer repo.Free()

			commitOid, err := git.NewOid(response)
			require.NoError(t, err)

			commit, err := repo.LookupCommit(commitOid)
			require.NoError(t, err)

			tree, err := commit.Tree()
			require.NoError(t, err)
			require.Len(t, tc.expected, int(tree.EntryCount()))

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
