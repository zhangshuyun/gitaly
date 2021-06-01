// +build static,system_libgit2

package main

import (
	"testing"
	"time"

	git "github.com/libgit2/git2go/v31"
	"github.com/stretchr/testify/require"
	cmdtesthelper "gitlab.com/gitlab-org/gitaly/v14/cmd/gitaly-git2go/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

var (
	masterRevision = "1e292f8fedd741b75372e19097c76d327140c312"
)

func TestRebase_validation(t *testing.T) {
	cfg, _, repoPath := testcfg.BuildWithRepo(t)
	testhelper.ConfigureGitalyGit2GoBin(t, cfg)
	committer := git2go.NewSignature("Foo", "foo@example.com", time.Now())

	testcases := []struct {
		desc        string
		request     git2go.RebaseCommand
		expectedErr string
	}{
		{
			desc:        "no arguments",
			expectedErr: "rebase: missing repository",
		},
		{
			desc:        "missing repository",
			request:     git2go.RebaseCommand{Committer: committer, BranchName: "feature", UpstreamRevision: masterRevision},
			expectedErr: "rebase: missing repository",
		},
		{
			desc:        "missing committer name",
			request:     git2go.RebaseCommand{Repository: repoPath, Committer: git2go.Signature{Email: "foo@example.com"}, BranchName: "feature", UpstreamRevision: masterRevision},
			expectedErr: "rebase: missing committer name",
		},
		{
			desc:        "missing committer email",
			request:     git2go.RebaseCommand{Repository: repoPath, Committer: git2go.Signature{Name: "Foo"}, BranchName: "feature", UpstreamRevision: masterRevision},
			expectedErr: "rebase: missing committer email",
		},
		{
			desc:        "missing branch name",
			request:     git2go.RebaseCommand{Repository: repoPath, Committer: committer, UpstreamRevision: masterRevision},
			expectedErr: "rebase: missing branch name",
		},
		{
			desc:        "missing upstream branch",
			request:     git2go.RebaseCommand{Repository: repoPath, Committer: committer, BranchName: "feature"},
			expectedErr: "rebase: missing upstream revision",
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

func TestRebase_rebase(t *testing.T) {
	testcases := []struct {
		desc         string
		branch       string
		commitsAhead int
		setupRepo    func(testing.TB, *git.Repository)
		expected     string
		expectedErr  string
	}{
		{
			desc:         "Single commit rebase",
			branch:       "gitaly-rename-test",
			commitsAhead: 1,
			expected:     "a08ed4bc45f9e686db93c5d0519f63d7b537270c",
		},
		{
			desc:         "Multiple commits",
			branch:       "csv",
			commitsAhead: 5,
			expected:     "2f8365edc69d3683e22c4209ae9641642d84dd4a",
		},
		{
			desc:         "Branch zero commits behind",
			branch:       "sha-starting-with-large-number",
			commitsAhead: 1,
			expected:     "842616594688d2351480dfebd67b3d8d15571e6d",
		},
		{
			desc:     "Merged branch",
			branch:   "branch-merged",
			expected: masterRevision,
		},
		{
			desc:   "Partially merged branch",
			branch: "branch-merged-plus-one",
			setupRepo: func(t testing.TB, repo *git.Repository) {
				head, err := lookupCommit(repo, "branch-merged")
				require.NoError(t, err)

				other, err := lookupCommit(repo, "gitaly-rename-test")
				require.NoError(t, err)
				tree, err := other.Tree()
				require.NoError(t, err)
				newOid, err := repo.CreateCommitFromIds("refs/heads/branch-merged-plus-one", &cmdtesthelper.DefaultAuthor, &cmdtesthelper.DefaultAuthor, "Message", tree.Object.Id(), head.Object.Id())
				require.NoError(t, err)
				require.Equal(t, "5da601ef10e314884bbade9d5b063be37579ccf9", newOid.String())
			},
			commitsAhead: 1,
			expected:     "591b29084164bcc58fa4fb851a3c409290b17bfe",
		},
		{
			desc:   "With upstream merged into",
			branch: "csv-plus-merge",
			setupRepo: func(t testing.TB, repo *git.Repository) {
				ours, err := lookupCommit(repo, "csv")
				require.NoError(t, err)
				theirs, err := lookupCommit(repo, "b83d6e391c22777fca1ed3012fce84f633d7fed0")
				require.NoError(t, err)

				index, err := repo.MergeCommits(ours, theirs, nil)
				require.NoError(t, err)
				tree, err := index.WriteTreeTo(repo)
				require.NoError(t, err)

				newOid, err := repo.CreateCommitFromIds("refs/heads/csv-plus-merge", &cmdtesthelper.DefaultAuthor, &cmdtesthelper.DefaultAuthor, "Message", tree, ours.Object.Id(), theirs.Object.Id())
				require.NoError(t, err)
				require.Equal(t, "5cfe4a597b54c8f2b7ae85212f67599a1492009c", newOid.String())
			},
			commitsAhead: 5, // Same as "Multiple commits"
			expected:     "2f8365edc69d3683e22c4209ae9641642d84dd4a",
		},
		{
			desc:        "Rebase with conflict",
			branch:      "rebase-encoding-failure-trigger",
			expectedErr: "rebase: commit \"eb8f5fb9523b868cef583e09d4bf70b99d2dd404\": conflicts have not been resolved",
		},
		{
			desc:        "Orphaned branch",
			branch:      "orphaned-branch",
			expectedErr: "rebase: find merge base: no merge base found",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			committer := git2go.NewSignature(string(gittest.TestUser.Name),
				string(gittest.TestUser.Email),
				time.Date(2021, 3, 1, 13, 45, 50, 0, time.FixedZone("", +2*60*60)))

			cfg, _, repoPath := testcfg.BuildWithRepo(t)
			testhelper.ConfigureGitalyGit2GoBin(t, cfg)

			repo, err := git.OpenRepository(repoPath)
			require.NoError(t, err)

			if tc.setupRepo != nil {
				tc.setupRepo(t, repo)
			}

			request := git2go.RebaseCommand{
				Repository:       repoPath,
				Committer:        committer,
				BranchName:       tc.branch,
				UpstreamRevision: masterRevision,
			}

			response, err := request.Run(ctx, cfg)
			if tc.expectedErr != "" {
				require.EqualError(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)

				result := response.String()
				require.Equal(t, tc.expected, result)

				commit, err := lookupCommit(repo, result)
				require.NoError(t, err)

				for i := tc.commitsAhead; i > 0; i-- {
					commit = commit.Parent(0)
				}
				masterCommit, err := lookupCommit(repo, masterRevision)
				require.NoError(t, err)
				require.Equal(t, masterCommit, commit)
			}
		})
	}
}
