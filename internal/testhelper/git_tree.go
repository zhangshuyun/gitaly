package testhelper

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TreeEntry represents an entry of a git tree object.
type TreeEntry struct {
	// Mode is the file mode of the tree entry.
	Mode string
	// Path is the full path of the tree entry.
	Path string
	// Content is the content of the tree entry.
	Content string
}

// RequireTree looks up the given treeish and asserts that its entries match
// the given expected entries. Tree entries are checked recursively.
func RequireTree(t testing.TB, repoPath, treeish string, expectedEntries []TreeEntry) {
	t.Helper()

	var actualEntries []TreeEntry

	output := bytes.TrimSpace(MustRunCommand(t, nil, "git", "-C", repoPath, "ls-tree", "-r", treeish))

	if len(output) > 0 {
		for _, line := range bytes.Split(output, []byte("\n")) {
			// Format: <mode> SP <type> SP <object> TAB <file>
			tabSplit := bytes.Split(line, []byte("\t"))
			spaceSplit := bytes.Split(tabSplit[0], []byte(" "))
			path := string(tabSplit[1])
			actualEntries = append(actualEntries, TreeEntry{
				Mode:    string(spaceSplit[0]),
				Path:    path,
				Content: string(MustRunCommand(t, nil, "git", "-C", repoPath, "show", treeish+":"+path)),
			})
		}
	}

	require.Equal(t, expectedEntries, actualEntries)
}
