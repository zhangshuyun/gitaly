package commit

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	pathPkg "path"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type revisionPath struct{ revision, path string }

// TreeEntryFinder is a struct for searching through a tree with caching.
type TreeEntryFinder struct {
	c         catfile.Batch
	treeCache map[revisionPath][]*gitalypb.TreeEntry
}

// NewTreeEntryFinder initializes a TreeEntryFinder with an empty tree cache.
func NewTreeEntryFinder(c catfile.Batch) *TreeEntryFinder {
	return &TreeEntryFinder{
		c:         c,
		treeCache: make(map[revisionPath][]*gitalypb.TreeEntry),
	}
}

// FindByRevisionAndPath returns a TreeEntry struct for the object present at the revision/path pair.
func (tef *TreeEntryFinder) FindByRevisionAndPath(ctx context.Context, revision, path string) (*gitalypb.TreeEntry, error) {
	dir := pathPkg.Dir(path)
	cacheKey := revisionPath{revision: revision, path: dir}
	entries, ok := tef.treeCache[cacheKey]

	if !ok {
		var err error
		entries, err = treeEntries(ctx, tef.c, revision, dir, "", false)
		if err != nil {
			return nil, err
		}

		tef.treeCache[cacheKey] = entries
	}

	for _, entry := range entries {
		if string(entry.Path) == path {
			return entry, nil
		}
	}

	return nil, nil
}

const (
	oidSize                  = sha1.Size
	defaultFlatTreeRecursion = 10
	maxFlattenedTrees        = 100
)

func extractEntryInfoFromTreeData(treeData io.Reader, commitOid, rootOid, rootPath, oid string) ([]*gitalypb.TreeEntry, error) {
	if len(oid) == 0 {
		return nil, fmt.Errorf("empty tree oid")
	}

	bufReader := bufio.NewReader(treeData)

	var entries []*gitalypb.TreeEntry
	oidBuf := &bytes.Buffer{}

	for {
		modeBytes, err := bufReader.ReadBytes(' ')
		if err == io.EOF {
			break
		}
		if err != nil || len(modeBytes) <= 1 {
			return nil, fmt.Errorf("read entry mode: %v", err)
		}
		modeBytes = modeBytes[:len(modeBytes)-1]

		filename, err := bufReader.ReadBytes('\x00')
		if err != nil || len(filename) <= 1 {
			return nil, fmt.Errorf("read entry path: %v", err)
		}
		filename = filename[:len(filename)-1]

		oidBuf.Reset()
		if _, err := io.CopyN(oidBuf, bufReader, oidSize); err != nil {
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

func treeEntries(ctx context.Context, c catfile.Batch, revision, path string, rootOid string, recursive bool) ([]*gitalypb.TreeEntry, error) {
	if path == "." {
		path = ""
	}

	// If we ask 'git cat-file' for a path outside the repository tree it
	// blows up with a fatal error. So, we avoid asking for this.
	if strings.HasPrefix(filepath.Clean(path), "../") {
		return nil, nil
	}

	if len(rootOid) == 0 {
		rootTreeInfo, err := c.Info(ctx, git.Revision(revision+"^{tree}"))
		if err != nil {
			if catfile.IsNotFound(err) {
				return nil, nil
			}

			return nil, err
		}

		rootOid = rootTreeInfo.Oid.String()
	}

	treeObj, err := c.Tree(ctx, git.Revision(fmt.Sprintf("%s:%s", revision, path)))
	if err != nil {
		if catfile.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	entries, err := extractEntryInfoFromTreeData(treeObj, revision, rootOid, path, treeObj.Oid.String())
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
			subentries, err := treeEntries(ctx, c, revision, string(entry.Path), rootOid, true)
			if err != nil {
				return nil, err
			}

			orderedEntries = append(orderedEntries, subentries...)
		}
	}

	return orderedEntries, nil
}
