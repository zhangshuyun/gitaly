package commit

import (
	"bytes"
	"fmt"
	"io"
	pathPkg "path"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
)

const oidSize = 20

func extractEntryInfoFromTreeData(treeData *bytes.Buffer, commitOid, rootOid, rootPath string, treeInfo *catfile.ObjectInfo) ([]*gitalypb.TreeEntry, error) {
	if len(treeInfo.Oid) == 0 {
		return nil, fmt.Errorf("empty tree oid")
	}

	var entries []*gitalypb.TreeEntry
	oidBuf := &bytes.Buffer{}
	for treeData.Len() > 0 {
		modeBytes, err := treeData.ReadBytes(' ')
		if err != nil || len(modeBytes) <= 1 {
			return nil, fmt.Errorf("read entry mode: %v", err)
		}
		modeBytes = modeBytes[:len(modeBytes)-1]

		filename, err := treeData.ReadBytes('\x00')
		if err != nil || len(filename) <= 1 {
			return nil, fmt.Errorf("read entry path: %v", err)
		}
		filename = filename[:len(filename)-1]

		oidBuf.Reset()
		if _, err := io.CopyN(oidBuf, treeData, oidSize); err != nil {
			return nil, fmt.Errorf("read entry oid: %v", err)
		}

		treeEntry, err := newTreeEntry(commitOid, rootOid, rootPath, filename, oidBuf.Bytes(), modeBytes)
		if err != nil {
			return nil, fmt.Errorf("new entry info: %v", err)
		}

		entries = append(entries, treeEntry)
	}

	return entries, nil
}

type revisionPath struct{ revision, path string }

type TreeEntryFinder struct {
	c         *catfile.Batch
	treeCache map[revisionPath][]*gitalypb.TreeEntry
}

func NewTreeEntryFinder(c *catfile.Batch) *TreeEntryFinder {
	return &TreeEntryFinder{
		c:         c,
		treeCache: make(map[revisionPath][]*gitalypb.TreeEntry),
	}
}

func treeEntries(c *catfile.Batch, revision, path string, rootOid string, recursive bool) ([]*gitalypb.TreeEntry, error) {
	if path == "." {
		path = ""
	}

	// If we ask 'git cat-file' for a path outside the repository tree it
	// blows up with a fatal error. So, we avoid asking for this.
	if strings.HasPrefix(filepath.Clean(path), "../") {
		return nil, nil
	}

	if len(rootOid) == 0 {
		rootTreeInfo, err := c.Info(revision + "^{tree}")
		if err != nil {
			if catfile.IsNotFound(err) {
				return nil, nil
			}

			return nil, err
		}

		rootOid = rootTreeInfo.Oid
	}

	treeEntryInfo, err := c.Info(fmt.Sprintf("%s:%s", revision, path))
	if err != nil {
		if catfile.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	if treeEntryInfo.Type != "tree" {
		return nil, nil
	}

	treeReader, err := c.Tree(treeEntryInfo.Oid)
	if err != nil {
		return nil, err
	}

	treeBytes := &bytes.Buffer{}
	if _, err := treeBytes.ReadFrom(treeReader); err != nil {
		return nil, err
	}

	entries, err := extractEntryInfoFromTreeData(treeBytes, revision, rootOid, path, treeEntryInfo)
	if err != nil {
		return nil, err
	}

	if !recursive {
		return entries, nil
	}

	var orderedEntries []*gitalypb.TreeEntry
	for _, entry := range entries {
		orderedEntries = append(orderedEntries, entry)

		if entry.Type == gitalypb.TreeEntry_TREE {
			subentries, err := treeEntries(c, revision, string(entry.Path), rootOid, true)
			if err != nil {
				return nil, err
			}

			orderedEntries = append(orderedEntries, subentries...)
		}
	}

	return orderedEntries, nil
}

// TreeEntryForRevisionAndPath returns a TreeEntry struct for the object present at the revision/path pair.
func (tef *TreeEntryFinder) TreeEntryForRevisionAndPath(revision, path string) (*gitalypb.TreeEntry, error) {
	entries, err := treeEntries(tef.c, revision, pathPkg.Dir(path), "", false)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if string(entry.Path) == path {
			entry.RootOid = "" // Not sure why we do this
			return entry, nil
		}
	}

	return nil, nil
}
