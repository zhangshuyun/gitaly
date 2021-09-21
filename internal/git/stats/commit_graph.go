package stats

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	// CommitGraphRelPath is the path to the file that stores info about commit graph.
	CommitGraphRelPath = "objects/info/commit-graph"
	// CommitGraphsRelPath is the path to the folder that stores commit graph chain files.
	CommitGraphsRelPath = "objects/info/commit-graphs"
	// CommitGraphChainRelPath is the path to the file that stores info about commit graph chain files.
	CommitGraphChainRelPath = CommitGraphsRelPath + "/commit-graph-chain"
)

// IsMissingBloomFilters checks if the commit graph chain exists and has a Bloom filters indicators.
// If the chain contains multiple files, each one is checked until at least one is missing a Bloom filter
// indicator or until there are no more files to check.
// https://git-scm.com/docs/commit-graph#_file_layout
func IsMissingBloomFilters(repoPath string) (bool, error) {
	const chunkTableEntrySize = 12

	commitGraphsPath := filepath.Join(repoPath, CommitGraphChainRelPath)
	commitGraphsData, err := os.ReadFile(commitGraphsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, err
	}

	ids := bytes.Split(bytes.TrimSpace(commitGraphsData), []byte{'\n'})
	for _, id := range ids {
		graphFilePath := filepath.Join(repoPath, filepath.Dir(CommitGraphChainRelPath), fmt.Sprintf("graph-%s.graph", id))
		graphFile, err := os.Open(graphFilePath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// concurrently modified
				continue
			}
			return false, fmt.Errorf("read commit graph chain file: %w", err)
		}
		defer graphFile.Close()

		reader := bufio.NewReader(graphFile)
		// https://github.com/git/git/blob/a43a2e6/Documentation/technical/commit-graph-format.txt#L123
		header := []byte{
			0, 0, 0, 0, // 4-byte signature: The signature is: {'C', 'G', 'P', 'H'}
			0, // 1-byte version number: Currently, the only valid version is 1.
			0, // 1-byte Hash Version
			0, // 1-byte number (C) of "chunks"
			0, // 1-byte number (B) of base commit-graphs
		}

		if n, err := reader.Read(header); err != nil {
			return false, fmt.Errorf("read commit graph file %q header: %w", graphFilePath, err)
		} else if n != len(header) {
			return false, fmt.Errorf("commit graph file %q is too small, no header", graphFilePath)
		}

		if !bytes.Equal(header[:4], []byte("CGPH")) {
			return false, fmt.Errorf("commit graph file %q doesn't have signature", graphFilePath)
		}
		if header[4] != 1 {
			return false, fmt.Errorf("commit graph file %q has unsupported version number: %v", graphFilePath, header[4])
		}

		C := header[6] // number (C) of "chunks"
		table := make([]byte, (C+1)*chunkTableEntrySize)
		if n, err := reader.Read(table); err != nil {
			return false, fmt.Errorf("read commit graph file %q table of contents for the chunks: %w", graphFilePath, err)
		} else if n != len(table) {
			return false, fmt.Errorf("commit graph file %q is too small, no table of contents", graphFilePath)
		}

		if !bytes.Contains(table, []byte("BIDX")) && !bytes.Contains(table, []byte("BDAT")) {
			return true, nil
		}

		if err := graphFile.Close(); err != nil {
			return false, fmt.Errorf("commit graph file %q close: %w", graphFilePath, err)
		}
	}

	return false, nil
}
