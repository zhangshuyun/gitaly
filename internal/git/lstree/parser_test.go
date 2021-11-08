package lstree

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	testCases := []struct {
		desc     string
		filename string
		entries  Entries
	}{
		{
			desc:     "regular entries",
			filename: "testdata/z-lstree.txt",
			entries: Entries{
				{
					Mode: []byte("100644"),
					Type: Blob,
					Oid:  "dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82",
					Path: ".gitignore",
				},
				{
					Mode: []byte("100644"),
					Type: Blob,
					Oid:  "0792c58905eff3432b721f8c4a64363d8e28d9ae",
					Path: ".gitmodules",
				},
				{
					Mode: []byte("040000"),
					Type: Tree,
					Oid:  "3c122d2b7830eca25235131070602575cf8b41a1",
					Path: "encoding",
				},
				{
					Mode: []byte("160000"),
					Type: Submodule,
					Oid:  "79bceae69cb5750d6567b223597999bfa91cb3b9",
					Path: "gitlab-shell",
				},
			},
		},
		{
			desc:     "irregular path",
			filename: "testdata/z-lstree-irregular.txt",
			entries: Entries{
				{
					Mode: []byte("100644"),
					Type: Blob,
					Oid:  "dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82",
					Path: ".gitignore",
				},
				{
					Mode: []byte("100644"),
					Type: Blob,
					Oid:  "0792c58905eff3432b721f8c4a64363d8e28d9ae",
					Path: ".gitmodules",
				},
				{
					Mode: []byte("040000"),
					Type: Tree,
					Oid:  "3c122d2b7830eca25235131070602575cf8b41a1",
					Path: "some encoding",
				},
				{
					Mode: []byte("160000"),
					Type: Submodule,
					Oid:  "79bceae69cb5750d6567b223597999bfa91cb3b9",
					Path: "gitlab-shell",
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			file, err := os.Open(testCase.filename)

			require.NoError(t, err)
			defer file.Close()

			parsedEntries := Entries{}

			parser := NewParser(file)
			for {
				entry, err := parser.NextEntry()
				if err == io.EOF {
					break
				}

				require.NoError(t, err)
				parsedEntries = append(parsedEntries, *entry)
			}

			expectedEntries := testCase.entries
			require.Len(t, expectedEntries, len(parsedEntries))

			for index, parsedEntry := range parsedEntries {
				expectedEntry := expectedEntries[index]

				require.Equal(t, expectedEntry, parsedEntry)
			}
		})
	}
}
