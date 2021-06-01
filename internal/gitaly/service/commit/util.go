package commit

import (
	"fmt"
	"path/filepath"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func newTreeEntry(commitOid, rootOid, rootPath string, filename, oidBytes, modeBytes []byte) (*gitalypb.TreeEntry, error) {
	var objectType gitalypb.TreeEntry_EntryType

	mode, err := strconv.ParseInt(string(modeBytes), 8, 32)
	if err != nil {
		return nil, fmt.Errorf("parse mode: %v", err)
	}

	oid := fmt.Sprintf("%02x", oidBytes)

	// Based on https://github.com/git/git/blob/v2.13.1/builtin/ls-tree.c#L67-L87
	switch mode & 0xf000 {
	case 0160000:
		objectType = gitalypb.TreeEntry_COMMIT
	case 040000:
		objectType = gitalypb.TreeEntry_TREE
	default:
		objectType = gitalypb.TreeEntry_BLOB
	}

	return &gitalypb.TreeEntry{
		CommitOid: commitOid,
		RootOid:   rootOid,
		Oid:       oid,
		Path:      []byte(filepath.Join(rootPath, string(filename))),
		Type:      objectType,
		Mode:      int32(mode),
	}, nil
}
