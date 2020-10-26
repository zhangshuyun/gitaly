// +build static,system_libgit2

package command

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

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
		_, repoPath, cleanup := testhelper.NewTestRepo(t)
		defer cleanup()

		base := buildCommit(t, repoPath, nil, tc.base)
		ours := buildCommit(t, repoPath, base, tc.ours)
		theirs := buildCommit(t, repoPath, base, tc.theirs)

		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := git2go.ConflictsCommand{
				Repository: repoPath,
				Ours:       ours.String(),
				Theirs:     theirs.String(),
			}.Run(ctx, config.Config)

			require.NoError(t, err)
			require.Equal(t, tc.conflicts, response.Conflicts)
		})
	}
}
