package repository

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	CommitGraphRelPath      = "objects/info/commit-graph"
	CommitGraphsRelPath     = "objects/info/commit-graphs"
	CommitGraphChainRelPath = CommitGraphsRelPath + "/commit-graph-chain"
)

// WriteCommitGraph write or update commit-graph file in a repository
func (s *server) WriteCommitGraph(
	ctx context.Context,
	in *gitalypb.WriteCommitGraphRequest,
) (*gitalypb.WriteCommitGraphResponse, error) {
	if err := s.writeCommitGraph(ctx, in.GetRepository(), in.GetSplitStrategy()); err != nil {
		return nil, err
	}

	return &gitalypb.WriteCommitGraphResponse{}, nil
}

func (s *server) writeCommitGraph(
	ctx context.Context,
	repo repository.GitRepo,
	splitStrategy gitalypb.WriteCommitGraphRequest_SplitStrategy,
) error {
	repoPath, err := s.locator.GetRepoPath(repo)
	if err != nil {
		return err
	}

	missingBloomFilters := true
	if _, err := os.Stat(filepath.Join(repoPath, CommitGraphRelPath)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return helper.ErrInternal(fmt.Errorf("remove commit graph file: %w", err))
		}

		// objects/info/commit-graph file doesn't exists
		// check if commit-graph chain exists and includes Bloom filters
		if missingBloomFilters, err = isMissingBloomFilters(repoPath); err != nil {
			return helper.ErrInternal(fmt.Errorf("should remove commit graph chain: %w", err))
		}
	}

	flags := []git.Option{
		git.Flag{Name: "--reachable"},
		git.Flag{Name: "--changed-paths"}, // enables Bloom filters
	}

	if missingBloomFilters {
		// if commit graph doesn't use Bloom filters we instruct operation to replace
		// existent commit graph with the new one
		// https://git-scm.com/docs/git-commit-graph#Documentation/git-commit-graph.txt-emwriteem
		flags = append(flags, git.Flag{Name: "--split=replace"})
	} else {
		flags = append(flags, git.Flag{Name: "--split"})
	}

	switch splitStrategy {
	case gitalypb.WriteCommitGraphRequest_SizeMultiple:
		flags = append(flags,
			// this flag has no effect if '--split=replace' is used
			git.ValueFlag{Name: "--size-multiple", Value: "4"},
		)
	default:
		return helper.ErrInvalidArgumentf("unsupported split strategy: %v", splitStrategy)
	}

	var stderr bytes.Buffer
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.SubSubCmd{
			Name:   "commit-graph",
			Action: "write",
			Flags:  flags,
		},
		git.WithStderr(&stderr),
	)
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := cmd.Wait(); err != nil {
		return helper.ErrInternalf("commit-graph write: %s: %v", err, stderr.String())
	}

	return nil
}

// isMissingBloomFilters checks if the commit graph chain exists and has a Bloom filters indicators.
// If the chain contains multiple files, each one is checked until at least one is missing a Bloom filter
// indicator or until there are no more files to check.
// https://git-scm.com/docs/commit-graph#_file_layout
func isMissingBloomFilters(repoPath string) (bool, error) {
	const chunkTableEntrySize = 12

	commitGraphsPath := filepath.Join(repoPath, CommitGraphChainRelPath)
	commitGraphsData, err := ioutil.ReadFile(commitGraphsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
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
			0, 0, 0, 0, //4-byte signature: The signature is: {'C', 'G', 'P', 'H'}
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
