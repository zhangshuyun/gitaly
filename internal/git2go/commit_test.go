package git2go_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

type commit struct {
	Parent    string
	Author    git2go.Signature
	Committer git2go.Signature
	Message   string
}

func TestExecutor_Commit(t *testing.T) { testExecutors(t, testExecutor_Commit) }

func testExecutor_Commit(t *testing.T, executor git2go.Executor) {
	const (
		DefaultMode    = "100644"
		ExecutableMode = "100755"
	)

	type step struct {
		actions     []git2go.Action
		error       error
		treeEntries []testhelper.TreeEntry
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	pbRepo, repoPath, clean := testhelper.InitBareRepo(t)
	defer clean()

	repo := git.NewRepository(pbRepo)

	originalFile, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("original"))
	require.NoError(t, err)

	updatedFile, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("updated"))
	require.NoError(t, err)

	for _, tc := range []struct {
		desc  string
		steps []step
	}{
		{
			desc: "create directory",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateDirectory{Path: "directory"},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "directory/.gitkeep"},
					},
				},
			},
		},
		{
			desc: "create directory created duplicate",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateDirectory{Path: "directory"},
						git2go.CreateDirectory{Path: "directory"},
					},
					error: git2go.DirectoryExistsError("directory"),
				},
			},
		},
		{
			desc: "create directory existing duplicate",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateDirectory{Path: "directory"},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "directory/.gitkeep"},
					},
				},
				{
					actions: []git2go.Action{
						git2go.CreateDirectory{Path: "directory"},
					},
					error: git2go.DirectoryExistsError("directory"),
				},
			},
		},
		{
			desc: "create directory with a files name",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file", OID: originalFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "original"},
					},
				},
				{
					actions: []git2go.Action{
						git2go.CreateDirectory{Path: "file"},
					},
					error: git2go.FileExistsError("file"),
				},
			},
		},
		{
			desc: "create file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file", OID: originalFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "original"},
					},
				},
			},
		},
		{
			desc: "create duplicate file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file", OID: originalFile},
						git2go.CreateFile{Path: "file", OID: updatedFile},
					},
					error: git2go.FileExistsError("file"),
				},
			},
		},
		{
			desc: "create file overwrites directory",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateDirectory{Path: "directory"},
						git2go.CreateFile{Path: "directory", OID: originalFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "directory", Content: "original"},
					},
				},
			},
		},
		{
			desc: "update created file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file", OID: originalFile},
						git2go.UpdateFile{Path: "file", OID: updatedFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "updated"},
					},
				},
			},
		},
		{
			desc: "update existing file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file", OID: originalFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "original"},
					},
				},
				{
					actions: []git2go.Action{
						git2go.UpdateFile{Path: "file", OID: updatedFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "updated"},
					},
				},
			},
		},
		{
			desc: "update non-existing file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.UpdateFile{Path: "non-existing", OID: updatedFile},
					},
					error: git2go.FileNotFoundError("non-existing"),
				},
			},
		},
		{
			desc: "move created file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "original-file", OID: originalFile},
						git2go.MoveFile{Path: "original-file", NewPath: "moved-file", OID: originalFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "original"},
					},
				},
			},
		},
		{
			desc: "moving directory fails",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateDirectory{Path: "directory"},
						git2go.MoveFile{Path: "directory", NewPath: "moved-directory"},
					},
					error: git2go.FileNotFoundError("directory"),
				},
			},
		},
		{
			desc: "move file inferring content",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "original-file", OID: originalFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original"},
					},
				},
				{
					actions: []git2go.Action{
						git2go.MoveFile{Path: "original-file", NewPath: "moved-file"},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "original"},
					},
				},
			},
		},
		{
			desc: "move file with non-existing source",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.MoveFile{Path: "non-existing", NewPath: "destination-file"},
					},
					error: git2go.FileNotFoundError("non-existing"),
				},
			},
		},
		{
			desc: "move file with already existing destination file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "source-file", OID: originalFile},
						git2go.CreateFile{Path: "already-existing", OID: updatedFile},
						git2go.MoveFile{Path: "source-file", NewPath: "already-existing"},
					},
					error: git2go.FileExistsError("already-existing"),
				},
			},
		},
		{
			// seems like a bug in the original implementation to allow overwriting a
			// directory
			desc: "move file with already existing destination directory",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file", OID: originalFile},
						git2go.CreateDirectory{Path: "already-existing"},
						git2go.MoveFile{Path: "file", NewPath: "already-existing"},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "already-existing", Content: "original"},
					},
				},
			},
		},
		{
			desc: "move file providing content",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "original-file", OID: originalFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original"},
					},
				},
				{
					actions: []git2go.Action{
						git2go.MoveFile{Path: "original-file", NewPath: "moved-file", OID: updatedFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "updated"},
					},
				},
			},
		},
		{
			desc: "mark non-existing file executable",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.ChangeFileMode{Path: "non-existing"},
					},
					error: git2go.FileNotFoundError("non-existing"),
				},
			},
		},
		{
			desc: "mark executable file executable",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file-1", OID: originalFile},
						git2go.ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
				{
					actions: []git2go.Action{
						git2go.ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
			},
		},
		{
			desc: "mark created file executable",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file-1", OID: originalFile},
						git2go.ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
			},
		},
		{
			desc: "mark existing file executable",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file-1", OID: originalFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "original"},
					},
				},
				{
					actions: []git2go.Action{
						git2go.ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
			},
		},
		{
			desc: "move non-existing file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.MoveFile{Path: "non-existing", NewPath: "destination"},
					},
					error: git2go.FileNotFoundError("non-existing"),
				},
			},
		},
		{
			desc: "move doesn't overwrite a file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file-1", OID: originalFile},
						git2go.CreateFile{Path: "file-2", OID: updatedFile},
						git2go.MoveFile{Path: "file-1", NewPath: "file-2"},
					},
					error: git2go.FileExistsError("file-2"),
				},
			},
		},
		{
			desc: "delete non-existing file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.DeleteFile{Path: "non-existing"},
					},
					error: git2go.FileNotFoundError("non-existing"),
				},
			},
		},
		{
			desc: "delete created file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file-1", OID: originalFile},
						git2go.DeleteFile{Path: "file-1"},
					},
				},
			},
		},
		{
			desc: "delete existing file",
			steps: []step{
				{
					actions: []git2go.Action{
						git2go.CreateFile{Path: "file-1", OID: originalFile},
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "original"},
					},
				},
				{
					actions: []git2go.Action{
						git2go.DeleteFile{Path: "file-1"},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			author := git2go.NewSignature("Author Name", "author.email@example.com", time.Now())
			var parentCommit string
			for i, step := range tc.steps {
				message := fmt.Sprintf("commit %d", i+1)
				commitID, err := executor.Commit(ctx, git2go.CommitParams{
					Repository: repoPath,
					Author:     author,
					Message:    message,
					Parent:     parentCommit,
					Actions:    step.actions,
				})

				if step.error != nil {
					require.True(t, errors.Is(err, step.error), "expected: %q, actual: %q", step.error, err)
					continue
				} else {
					require.NoError(t, err)
				}

				require.Equal(t, commit{
					Parent:    parentCommit,
					Author:    author,
					Committer: author,
					Message:   message,
				}, getCommit(t, ctx, repo, commitID))

				testhelper.RequireTree(t, repoPath, commitID, step.treeEntries)
				parentCommit = commitID
			}
		})
	}
}

func getCommit(t testing.TB, ctx context.Context, repo git.Repository, oid string) commit {
	t.Helper()

	data, err := repo.ReadObject(ctx, oid)
	require.NoError(t, err)

	var commit commit
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		if line == "" {
			commit.Message = strings.Join(lines[i+1:], "\n")
			break
		}

		split := strings.SplitN(line, " ", 2)
		require.Len(t, split, 2, "invalid commit: %q", data)

		field, value := split[0], split[1]
		switch field {
		case "parent":
			require.Empty(t, commit.Parent, "multi parent parsing not implemented")
			commit.Parent = value
		case "author":
			require.Empty(t, commit.Author, "commit contained multiple authors")
			commit.Author = unmarshalSignature(t, value)
		case "committer":
			require.Empty(t, commit.Committer, "commit contained multiple committers")
			commit.Committer = unmarshalSignature(t, value)
		default:
		}
	}

	return commit
}

func unmarshalSignature(t testing.TB, data string) git2go.Signature {
	t.Helper()

	// Format: NAME <EMAIL> DATE_UNIX DATE_TIMEZONE
	split1 := strings.Split(data, " <")
	require.Len(t, split1, 2, "invalid signature: %q", data)

	split2 := strings.Split(split1[1], "> ")
	require.Len(t, split2, 2, "invalid signature: %q", data)

	split3 := strings.Split(split2[1], " ")
	require.Len(t, split3, 2, "invalid signature: %q", data)

	timestamp, err := strconv.ParseInt(split3[0], 10, 64)
	require.NoError(t, err)

	return git2go.Signature{
		Name:  split1[0],
		Email: split2[0],
		When:  time.Unix(timestamp, 0),
	}
}
