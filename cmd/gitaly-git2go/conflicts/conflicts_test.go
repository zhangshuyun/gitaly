// +build static,system_libgit2

package conflicts

import (
	"os"
	"testing"

	git "github.com/libgit2/git2go/v31"
	"github.com/stretchr/testify/require"
	cmdtesthelper "gitlab.com/gitlab-org/gitaly/cmd/gitaly-git2go/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func TestConflicts(t *testing.T) {
	testcases := []struct {
		desc      string
		base      map[string]string
		ours      map[string]string
		theirs    map[string]string
		conflicts []git2go.Conflict
	}{
		{
			desc: "no conflicts",
			base: map[string]string{
				"file": "a",
			},
			ours: map[string]string{
				"file": "a",
			},
			theirs: map[string]string{
				"file": "b",
			},
			conflicts: nil,
		},
		{
			desc: "single file",
			base: map[string]string{
				"file": "a",
			},
			ours: map[string]string{
				"file": "b",
			},
			theirs: map[string]string{
				"file": "c",
			},
			conflicts: []git2go.Conflict{
				{
					Ancestor: git2go.ConflictEntry{Path: "file", Mode: 0100644},
					Our:      git2go.ConflictEntry{Path: "file", Mode: 0100644},
					Their:    git2go.ConflictEntry{Path: "file", Mode: 0100644},
					Content:  []byte("<<<<<<< file\nb\n=======\nc\n>>>>>>> file\n"),
				},
			},
		},
		{
			desc: "multiple files with single conflict",
			base: map[string]string{
				"file-1": "a",
				"file-2": "a",
			},
			ours: map[string]string{
				"file-1": "b",
				"file-2": "b",
			},
			theirs: map[string]string{
				"file-1": "a",
				"file-2": "c",
			},
			conflicts: []git2go.Conflict{
				{
					Ancestor: git2go.ConflictEntry{Path: "file-2", Mode: 0100644},
					Our:      git2go.ConflictEntry{Path: "file-2", Mode: 0100644},
					Their:    git2go.ConflictEntry{Path: "file-2", Mode: 0100644},
					Content:  []byte("<<<<<<< file-2\nb\n=======\nc\n>>>>>>> file-2\n"),
				},
			},
		},
		{
			desc: "multiple conflicts",
			base: map[string]string{
				"file-1": "a",
				"file-2": "a",
			},
			ours: map[string]string{
				"file-1": "b",
				"file-2": "b",
			},
			theirs: map[string]string{
				"file-1": "c",
				"file-2": "c",
			},
			conflicts: []git2go.Conflict{
				{
					Ancestor: git2go.ConflictEntry{Path: "file-1", Mode: 0100644},
					Our:      git2go.ConflictEntry{Path: "file-1", Mode: 0100644},
					Their:    git2go.ConflictEntry{Path: "file-1", Mode: 0100644},
					Content:  []byte("<<<<<<< file-1\nb\n=======\nc\n>>>>>>> file-1\n"),
				},
				{
					Ancestor: git2go.ConflictEntry{Path: "file-2", Mode: 0100644},
					Our:      git2go.ConflictEntry{Path: "file-2", Mode: 0100644},
					Their:    git2go.ConflictEntry{Path: "file-2", Mode: 0100644},
					Content:  []byte("<<<<<<< file-2\nb\n=======\nc\n>>>>>>> file-2\n"),
				},
			},
		},
		{
			desc: "modified-delete-conflict",
			base: map[string]string{
				"file": "content",
			},
			ours: map[string]string{
				"file": "changed",
			},
			theirs: map[string]string{
				"different-file": "unrelated",
			},
			conflicts: []git2go.Conflict{
				{
					Ancestor: git2go.ConflictEntry{Path: "file", Mode: 0100644},
					Our:      git2go.ConflictEntry{Path: "file", Mode: 0100644},
					Their:    git2go.ConflictEntry{},
					Content:  []byte("<<<<<<< file\nchanged\n=======\n>>>>>>> \n"),
				},
			},
		},
		{
			// Ruby code doesn't call `merge_commits` with rename
			// detection and so don't we. The rename conflict is
			// thus split up into three conflicts.
			desc: "rename-rename-conflict",
			base: map[string]string{
				"file": "a\nb\nc\nd\ne\nf\ng\n",
			},
			ours: map[string]string{
				"renamed-1": "a\nb\nc\nd\ne\nf\ng\n",
			},
			theirs: map[string]string{
				"renamed-2": "a\nb\nc\nd\ne\nf\ng\n",
			},
			conflicts: []git2go.Conflict{
				{
					Ancestor: git2go.ConflictEntry{Path: "file", Mode: 0100644},
					Our:      git2go.ConflictEntry{},
					Their:    git2go.ConflictEntry{},
					Content:  []byte{},
				},
				{
					Ancestor: git2go.ConflictEntry{},
					Our:      git2go.ConflictEntry{Path: "renamed-1", Mode: 0100644},
					Their:    git2go.ConflictEntry{},
					Content:  []byte("a\nb\nc\nd\ne\nf\ng\n"),
				},
				{
					Ancestor: git2go.ConflictEntry{},
					Our:      git2go.ConflictEntry{},
					Their:    git2go.ConflictEntry{Path: "renamed-2", Mode: 0100644},
					Content:  []byte("a\nb\nc\nd\ne\nf\ng\n"),
				},
			},
		},
	}

	for _, tc := range testcases {
		cfg, _, repoPath := testcfg.BuildWithRepo(t)

		testhelper.ConfigureGitalyGit2GoBin(t, cfg)

		base := cmdtesthelper.BuildCommit(t, repoPath, nil, tc.base)
		ours := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, tc.ours)
		theirs := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, tc.theirs)

		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := git2go.ConflictsCommand{
				Repository: repoPath,
				Ours:       ours.String(),
				Theirs:     theirs.String(),
			}.Run(ctx, cfg)

			require.NoError(t, err)
			require.Equal(t, tc.conflicts, response.Conflicts)
		})
	}
}
