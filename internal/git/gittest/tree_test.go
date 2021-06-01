package gittest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
)

func TestWriteTree(t *testing.T) {
	cfg, _, repoPath := setup(t)

	blobID := WriteBlob(t, cfg, repoPath, []byte("foobar\n"))
	treeID := WriteTree(t, cfg, repoPath, []TreeEntry{
		{
			OID:  blobID,
			Mode: "100644",
			Path: "file",
		},
	})

	for _, tc := range []struct {
		desc            string
		entries         []TreeEntry
		expectedEntries []TreeEntry
		expectedOID     git.ObjectID
	}{
		{
			desc: "entry with blob OID",
			entries: []TreeEntry{
				{
					OID:  blobID,
					Mode: "100644",
					Path: "file",
				},
			},
			expectedEntries: []TreeEntry{
				{
					OID:     blobID,
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "file",
				},
			},
			expectedOID: "54a22f36d78d0ba7964f71ff72c7309edecab857",
		},
		{
			desc: "entry with blob content",
			entries: []TreeEntry{
				{
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "file",
				},
			},
			expectedEntries: []TreeEntry{
				{
					OID:     "323fae03f4606ea9991df8befbb2fca795e648fa",
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "file",
				},
			},
			expectedOID: "54a22f36d78d0ba7964f71ff72c7309edecab857",
		},
		{
			desc: "entry with tree OID",
			entries: []TreeEntry{
				{
					OID:  treeID,
					Mode: "040000",
					Path: "dir",
				},
			},
			expectedEntries: []TreeEntry{
				{
					OID:     blobID,
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "dir/file",
				},
			},
			expectedOID: "c69f8fc9c97fcae2a80ba1578c493171984d810a",
		},
		{
			desc: "mixed tree and blob entries",
			entries: []TreeEntry{
				{
					OID:  treeID,
					Mode: "040000",
					Path: "dir",
				},
				{
					OID:  blobID,
					Mode: "100644",
					Path: "file1",
				},
				{
					Content: "different content",
					Mode:    "100644",
					Path:    "file2",
				},
			},
			expectedEntries: []TreeEntry{
				{
					OID:     blobID,
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "dir/file",
				},
				{
					OID:     blobID,
					Content: "foobar\n",
					Mode:    "100644",
					Path:    "file1",
				},
				{
					OID:     "9b62abfb7f69b6d5801a232a9e6c332a10c9cafc",
					Content: "different content",
					Mode:    "100644",
					Path:    "file2",
				},
			},
			expectedOID: "70a96b29b67eb29344f399c1c4bc0047568e8dba",
		},
		{
			desc: "two entries with nonexistant objects",
			entries: []TreeEntry{
				{
					OID:  git.ObjectID(strings.Repeat("1", 40)),
					Mode: "100644",
					Path: "file",
				},
				{
					OID:  git.ObjectID(strings.Repeat("0", 40)),
					Mode: "100644",
					Path: "file",
				},
			},
			expectedOID: "09e7f53dec572807e651fc368d834f9744a5a42c",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			oid := WriteTree(t, cfg, repoPath, tc.entries)

			if tc.expectedEntries != nil {
				RequireTree(t, cfg, repoPath, oid.String(), tc.expectedEntries)
			}

			require.Equal(t, tc.expectedOID, oid)
		})
	}
}
