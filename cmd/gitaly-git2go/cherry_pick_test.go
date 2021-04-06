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

func TestCherryPick_validation(t *testing.T) {
	cfg, _, repoPath := testcfg.BuildWithRepo(t)
	testhelper.ConfigureGitalyGit2GoBin(t, cfg)

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
			request:     git2go.CherryPickCommand{CommitterName: "Foo", CommitterMail: "foo@example.com", CommitterDate: time.Now(), Message: "Foo", Ours: "HEAD", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing repository",
		},
		{
			desc:        "missing committer name",
			request:     git2go.CherryPickCommand{Repository: repoPath, CommitterMail: "foo@example.com", CommitterDate: time.Now(), Message: "Foo", Ours: "HEAD", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing committer name",
		},
		{
			desc:        "missing committer mail",
			request:     git2go.CherryPickCommand{Repository: repoPath, CommitterName: "Foo", CommitterDate: time.Now(), Message: "Foo", Ours: "HEAD", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing committer mail",
		},
		{
			desc:        "missing committer date",
			request:     git2go.CherryPickCommand{Repository: repoPath, CommitterName: "Foo", CommitterMail: "foo@example.com", Message: "Foo", Ours: "HEAD", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing committer date",
		},
		{
			desc:        "missing message",
			request:     git2go.CherryPickCommand{Repository: repoPath, CommitterName: "Foo", CommitterMail: "foo@example.com", CommitterDate: time.Now(), Ours: "HEAD", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing message",
		},
		{
			desc:        "missing ours",
			request:     git2go.CherryPickCommand{Repository: repoPath, CommitterName: "Foo", CommitterMail: "foo@example.com", CommitterDate: time.Now(), Message: "Foo", Commit: "HEAD"},
			expectedErr: "cherry-pick: missing ours",
		},
		{
			desc:        "missing commit",
			request:     git2go.CherryPickCommand{Repository: repoPath, CommitterName: "Foo", CommitterMail: "foo@example.com", CommitterDate: time.Now(), Message: "Foo", Ours: "HEAD"},
			expectedErr: "cherry-pick: missing commit",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := tc.request.Run(ctx, cfg)
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
			expectedCommitID: "aa3c9f5ad67ad86e313129a851f6d64614be7f6e",
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
			desc: "empty cherry-pick fails",
			base: map[string]string{
				"file": "foo",
			},
			ours: map[string]string{
				"file": "fooqux",
			},
			commit: map[string]string{
				"file": "fooqux",
			},
			expectedErr:    git2go.EmptyError{},
			expectedErrMsg: "cherry-pick: could not apply because the result was empty",
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
		cfg, _, repoPath := testcfg.BuildWithRepo(t)
		testhelper.ConfigureGitalyGit2GoBin(t, cfg)

		base := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{nil}, tc.base)

		var ours, commit = "nonexistent", "nonexistent"
		if len(tc.ours) > 0 {
			ours = cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, tc.ours).String()
		}
		if len(tc.commit) > 0 {
			commit = cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, tc.commit).String()
		}

		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			committer := git.Signature{
				Name:  "Baz",
				Email: "baz@example.com",
				When:  time.Date(2021, 1, 17, 14, 45, 51, 0, time.FixedZone("", +2*60*60)),
			}

			response, err := git2go.CherryPickCommand{
				Repository:    repoPath,
				CommitterName: committer.Name,
				CommitterMail: committer.Email,
				CommitterDate: committer.When,
				Message:       "Foo",
				Ours:          ours,
				Commit:        commit,
			}.Run(ctx, cfg)

			if tc.expectedErrMsg != "" {
				require.EqualError(t, err, tc.expectedErrMsg)

				if tc.expectedErr != nil {
					require.True(t, errors.Is(err, tc.expectedErr))
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
			require.Equal(t, &cmdtesthelper.DefaultAuthor, commit.Author())
			require.Equal(t, &committer, commit.Committer())

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
