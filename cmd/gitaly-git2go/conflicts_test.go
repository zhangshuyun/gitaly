//go:build static && system_libgit2
// +build static,system_libgit2

package main

import (
	"fmt"
	"testing"

	git "github.com/libgit2/git2go/v32"
	"github.com/stretchr/testify/require"
	cmdtesthelper "gitlab.com/gitlab-org/gitaly/v14/cmd/gitaly-git2go/testhelper"
	glgit "gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
					Ancestor: git2go.ConflictEntry{Path: "file", Mode: 0o100644},
					Our:      git2go.ConflictEntry{Path: "file", Mode: 0o100644},
					Their:    git2go.ConflictEntry{Path: "file", Mode: 0o100644},
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
					Ancestor: git2go.ConflictEntry{Path: "file-2", Mode: 0o100644},
					Our:      git2go.ConflictEntry{Path: "file-2", Mode: 0o100644},
					Their:    git2go.ConflictEntry{Path: "file-2", Mode: 0o100644},
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
					Ancestor: git2go.ConflictEntry{Path: "file-1", Mode: 0o100644},
					Our:      git2go.ConflictEntry{Path: "file-1", Mode: 0o100644},
					Their:    git2go.ConflictEntry{Path: "file-1", Mode: 0o100644},
					Content:  []byte("<<<<<<< file-1\nb\n=======\nc\n>>>>>>> file-1\n"),
				},
				{
					Ancestor: git2go.ConflictEntry{Path: "file-2", Mode: 0o100644},
					Our:      git2go.ConflictEntry{Path: "file-2", Mode: 0o100644},
					Their:    git2go.ConflictEntry{Path: "file-2", Mode: 0o100644},
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
					Ancestor: git2go.ConflictEntry{Path: "file", Mode: 0o100644},
					Our:      git2go.ConflictEntry{Path: "file", Mode: 0o100644},
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
					Ancestor: git2go.ConflictEntry{Path: "file", Mode: 0o100644},
					Our:      git2go.ConflictEntry{},
					Their:    git2go.ConflictEntry{},
					Content:  nil,
				},
				{
					Ancestor: git2go.ConflictEntry{},
					Our:      git2go.ConflictEntry{Path: "renamed-1", Mode: 0o100644},
					Their:    git2go.ConflictEntry{},
					Content:  []byte("a\nb\nc\nd\ne\nf\ng\n"),
				},
				{
					Ancestor: git2go.ConflictEntry{},
					Our:      git2go.ConflictEntry{},
					Their:    git2go.ConflictEntry{Path: "renamed-2", Mode: 0o100644},
					Content:  []byte("a\nb\nc\nd\ne\nf\ng\n"),
				},
			},
		},
	}

	for _, tc := range testcases {
		cfg, repo, repoPath := testcfg.BuildWithRepo(t)
		executor := git2go.NewExecutor(cfg, config.NewLocator(cfg))

		testcfg.BuildGitalyGit2Go(t, cfg)

		base := cmdtesthelper.BuildCommit(t, repoPath, nil, tc.base)
		ours := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, tc.ours)
		theirs := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, tc.theirs)

		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := executor.Conflicts(ctx, repo, git2go.ConflictsCommand{
				Repository: repoPath,
				Ours:       ours.String(),
				Theirs:     theirs.String(),
			})

			require.NoError(t, err)
			require.Equal(t, tc.conflicts, response.Conflicts)
		})
	}
}

func TestConflicts_checkError(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	base := cmdtesthelper.BuildCommit(t, repoPath, nil, nil)
	validOID := glgit.ObjectID(base.String())
	executor := git2go.NewExecutor(cfg, config.NewLocator(cfg))

	testcfg.BuildGitalyGit2Go(t, cfg)

	testcases := []struct {
		desc             string
		overrideRepoPath string
		ours             glgit.ObjectID
		theirs           glgit.ObjectID
		expErr           error
	}{
		{
			desc:   "ours is not set",
			ours:   "",
			theirs: validOID,
			expErr: fmt.Errorf("conflicts: %w: missing ours", git2go.ErrInvalidArgument),
		},
		{
			desc:   "theirs is not set",
			ours:   validOID,
			theirs: "",
			expErr: fmt.Errorf("conflicts: %w: missing theirs", git2go.ErrInvalidArgument),
		},
		{
			desc:             "invalid repository",
			overrideRepoPath: "not/existing/path.git",
			ours:             validOID,
			theirs:           validOID,
			expErr:           status.Error(codes.Internal, "could not open repository: failed to resolve path 'not/existing/path.git': No such file or directory"),
		},
		{
			desc:   "ours is invalid",
			ours:   "1",
			theirs: validOID,
			expErr: status.Error(codes.InvalidArgument, "encoding/hex: odd length hex string"),
		},
		{
			desc:   "theirs is invalid",
			ours:   validOID,
			theirs: "1",
			expErr: status.Error(codes.InvalidArgument, "encoding/hex: odd length hex string"),
		},
		{
			desc:   "ours OID doesn't exist",
			ours:   glgit.ZeroOID,
			theirs: validOID,
			expErr: status.Error(codes.InvalidArgument, "odb: cannot read object: null OID cannot exist"),
		},
		{
			desc:   "invalid object type",
			ours:   glgit.EmptyTreeOID,
			theirs: validOID,
			expErr: status.Error(codes.InvalidArgument, "the requested type does not match the type in the ODB"),
		},
		{
			desc:   "theirs OID doesn't exist",
			ours:   validOID,
			theirs: glgit.ZeroOID,
			expErr: status.Error(codes.InvalidArgument, "odb: cannot read object: null OID cannot exist"),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			repoPath := repoPath
			if tc.overrideRepoPath != "" {
				repoPath = tc.overrideRepoPath
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := executor.Conflicts(ctx, repo, git2go.ConflictsCommand{
				Repository: repoPath,
				Ours:       tc.ours.String(),
				Theirs:     tc.theirs.String(),
			})

			require.Error(t, err)
			require.Equal(t, tc.expErr, err)
		})
	}
}
