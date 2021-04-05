package git2go

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
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

type commit struct {
	Parent    git.ObjectID
	Author    Signature
	Committer Signature
	Message   string
}

func TestExecutor_Commit(t *testing.T) {
	const (
		DefaultMode    = "100644"
		ExecutableMode = "100755"
	)

	type step struct {
		actions     []Action
		error       error
		treeEntries []gittest.TreeEntry
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg := testcfg.Build(t)
	testhelper.ConfigureGitalyGit2GoBin(t, cfg)

	repoProto, repoPath, cleanup := gittest.InitBareRepoAt(t, cfg.Storages[0])
	t.Cleanup(cleanup)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	originalFile, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("original"))
	require.NoError(t, err)

	updatedFile, err := repo.WriteBlob(ctx, "file", bytes.NewBufferString("updated"))
	require.NoError(t, err)

	executor := New(filepath.Join(cfg.BinDir, "gitaly-git2go"), cfg.Git.BinPath)

	for _, tc := range []struct {
		desc  string
		steps []step
	}{
		{
			desc: "create directory",
			steps: []step{
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "directory/.gitkeep"},
					},
				},
			},
		},
		{
			desc: "create directory created duplicate",
			steps: []step{
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
						CreateDirectory{Path: "directory"},
					},
					error: DirectoryExistsError("directory"),
				},
			},
		},
		{
			desc: "create directory existing duplicate",
			steps: []step{
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "directory/.gitkeep"},
					},
				},
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
					},
					error: DirectoryExistsError("directory"),
				},
			},
		},
		{
			desc: "create directory with a files name",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "original"},
					},
				},
				{
					actions: []Action{
						CreateDirectory{Path: "file"},
					},
					error: FileExistsError("file"),
				},
			},
		},
		{
			desc: "create file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "original"},
					},
				},
			},
		},
		{
			desc: "create duplicate file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
						CreateFile{Path: "file", OID: updatedFile.String()},
					},
					error: FileExistsError("file"),
				},
			},
		},
		{
			desc: "create file overwrites directory",
			steps: []step{
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
						CreateFile{Path: "directory", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "directory", Content: "original"},
					},
				},
			},
		},
		{
			desc: "update created file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
						UpdateFile{Path: "file", OID: updatedFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "updated"},
					},
				},
			},
		},
		{
			desc: "update existing file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "original"},
					},
				},
				{
					actions: []Action{
						UpdateFile{Path: "file", OID: updatedFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file", Content: "updated"},
					},
				},
			},
		},
		{
			desc: "update non-existing file",
			steps: []step{
				{
					actions: []Action{
						UpdateFile{Path: "non-existing", OID: updatedFile.String()},
					},
					error: FileNotFoundError("non-existing"),
				},
			},
		},
		{
			desc: "move created file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "original-file", OID: originalFile.String()},
						MoveFile{Path: "original-file", NewPath: "moved-file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "original"},
					},
				},
			},
		},
		{
			desc: "moving directory fails",
			steps: []step{
				{
					actions: []Action{
						CreateDirectory{Path: "directory"},
						MoveFile{Path: "directory", NewPath: "moved-directory"},
					},
					error: FileNotFoundError("directory"),
				},
			},
		},
		{
			desc: "move file inferring content",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "original-file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original"},
					},
				},
				{
					actions: []Action{
						MoveFile{Path: "original-file", NewPath: "moved-file"},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "original"},
					},
				},
			},
		},
		{
			desc: "move file with non-existing source",
			steps: []step{
				{
					actions: []Action{
						MoveFile{Path: "non-existing", NewPath: "destination-file"},
					},
					error: FileNotFoundError("non-existing"),
				},
			},
		},
		{
			desc: "move file with already existing destination file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "source-file", OID: originalFile.String()},
						CreateFile{Path: "already-existing", OID: updatedFile.String()},
						MoveFile{Path: "source-file", NewPath: "already-existing"},
					},
					error: FileExistsError("already-existing"),
				},
			},
		},
		{
			// seems like a bug in the original implementation to allow overwriting a
			// directory
			desc: "move file with already existing destination directory",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file", OID: originalFile.String()},
						CreateDirectory{Path: "already-existing"},
						MoveFile{Path: "file", NewPath: "already-existing"},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "already-existing", Content: "original"},
					},
				},
			},
		},
		{
			desc: "move file providing content",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "original-file", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original"},
					},
				},
				{
					actions: []Action{
						MoveFile{Path: "original-file", NewPath: "moved-file", OID: updatedFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "updated"},
					},
				},
			},
		},
		{
			desc: "mark non-existing file executable",
			steps: []step{
				{
					actions: []Action{
						ChangeFileMode{Path: "non-existing"},
					},
					error: FileNotFoundError("non-existing"),
				},
			},
		},
		{
			desc: "mark executable file executable",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
						ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
				{
					actions: []Action{
						ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
			},
		},
		{
			desc: "mark created file executable",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
						ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
			},
		},
		{
			desc: "mark existing file executable",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "original"},
					},
				},
				{
					actions: []Action{
						ChangeFileMode{Path: "file-1", ExecutableMode: true},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "original"},
					},
				},
			},
		},
		{
			desc: "move non-existing file",
			steps: []step{
				{
					actions: []Action{
						MoveFile{Path: "non-existing", NewPath: "destination"},
					},
					error: FileNotFoundError("non-existing"),
				},
			},
		},
		{
			desc: "move doesn't overwrite a file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
						CreateFile{Path: "file-2", OID: updatedFile.String()},
						MoveFile{Path: "file-1", NewPath: "file-2"},
					},
					error: FileExistsError("file-2"),
				},
			},
		},
		{
			desc: "delete non-existing file",
			steps: []step{
				{
					actions: []Action{
						DeleteFile{Path: "non-existing"},
					},
					error: FileNotFoundError("non-existing"),
				},
			},
		},
		{
			desc: "delete created file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
						DeleteFile{Path: "file-1"},
					},
				},
			},
		},
		{
			desc: "delete existing file",
			steps: []step{
				{
					actions: []Action{
						CreateFile{Path: "file-1", OID: originalFile.String()},
					},
					treeEntries: []gittest.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "original"},
					},
				},
				{
					actions: []Action{
						DeleteFile{Path: "file-1"},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			author := NewSignature("Author Name", "author.email@example.com", time.Now())
			committer := NewSignature("Committer Name", "committer.email@example.com", time.Now())
			var parentCommit git.ObjectID
			for i, step := range tc.steps {
				message := fmt.Sprintf("commit %d", i+1)
				commitID, err := executor.Commit(ctx, CommitParams{
					Repository: repoPath,
					Author:     author,
					Committer:  committer,
					Message:    message,
					Parent:     parentCommit.String(),
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
					Committer: committer,
					Message:   message,
				}, getCommit(t, ctx, repo, commitID))

				gittest.RequireTree(t, repoPath, commitID.String(), step.treeEntries)
				parentCommit = commitID
			}
		})
	}
}

func getCommit(t testing.TB, ctx context.Context, repo *localrepo.Repo, oid git.ObjectID) commit {
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
			commit.Parent = git.ObjectID(value)
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

func unmarshalSignature(t testing.TB, data string) Signature {
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

	return Signature{
		Name:  split1[0],
		Email: split2[0],
		When:  time.Unix(timestamp, 0),
	}
}
