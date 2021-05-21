package gittest

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
)

// TreeEntry represents an entry of a git tree object.
type TreeEntry struct {
	// OID is the object ID the tree entry refers to.
	OID git.ObjectID
	// Mode is the file mode of the tree entry.
	Mode string
	// Path is the full path of the tree entry.
	Path string
	// Content is the content of the tree entry.
	Content string
}

// RequireTree looks up the given treeish and asserts that its entries match
// the given expected entries. Tree entries are checked recursively.
func RequireTree(t testing.TB, cfg config.Cfg, repoPath, treeish string, expectedEntries []TreeEntry) {
	t.Helper()

	for i, entry := range expectedEntries {
		if entry.OID != "" {
			continue
		}

		blob := fmt.Sprintf("blob %d\000%s", len(entry.Content), entry.Content)
		hash := sha1.Sum([]byte(blob))
		expectedEntries[i].OID = git.ObjectID(hex.EncodeToString(hash[:]))
	}

	var actualEntries []TreeEntry

	output := bytes.TrimSpace(Exec(t, cfg, "-C", repoPath, "ls-tree", "-r", treeish))

	if len(output) > 0 {
		for _, line := range bytes.Split(output, []byte("\n")) {
			// Format: <mode> SP <type> SP <object> TAB <file>
			tabSplit := bytes.Split(line, []byte("\t"))
			require.Len(t, tabSplit, 2)

			spaceSplit := bytes.Split(tabSplit[0], []byte(" "))
			require.Len(t, spaceSplit, 3)

			path := string(tabSplit[1])

			objectID, err := git.NewObjectIDFromHex(string(spaceSplit[2]))
			require.NoError(t, err)

			actualEntries = append(actualEntries, TreeEntry{
				OID:     objectID,
				Mode:    string(spaceSplit[0]),
				Path:    path,
				Content: string(Exec(t, cfg, "-C", repoPath, "show", treeish+":"+path)),
			})
		}
	}

	require.Equal(t, expectedEntries, actualEntries)
}

// WriteTree writes a new tree object to the given path. This function does not verify whether OIDs
// referred to by tree entries actually exist in the repository.
func WriteTree(t testing.TB, cfg config.Cfg, repoPath string, entries []TreeEntry) git.ObjectID {
	t.Helper()

	require.NotEmpty(t, entries)

	var tree bytes.Buffer
	for _, entry := range entries {
		var entryType string
		switch entry.Mode {
		case "100644":
			entryType = "blob"
		case "040000":
			entryType = "tree"
		default:
			t.Fatalf("invalid entry type %q", entry.Mode)
		}

		require.True(t, len(entry.OID) > 0 || len(entry.Content) > 0,
			"entry cannot have both OID and content")
		require.False(t, len(entry.OID) == 0 && len(entry.Content) == 0,
			"entry must have either an OID or content")

		oid := entry.OID
		if len(entry.Content) > 0 {
			oid = WriteBlob(t, cfg, repoPath, []byte(entry.Content))
		}

		formattedEntry := fmt.Sprintf("%s %s %s\t%s\000", entry.Mode, entryType, oid.String(), entry.Path)
		_, err := tree.WriteString(formattedEntry)
		require.NoError(t, err)
	}

	stdout := ExecStream(t, cfg, &tree, "-C", repoPath, "mktree", "-z", "--missing")
	treeOID, err := git.NewObjectIDFromHex(text.ChompBytes(stdout))
	require.NoError(t, err)

	return treeOID
}
