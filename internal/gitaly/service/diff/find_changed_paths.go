package diff

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	numStatDelimiter = 0
)

func (s *server) FindChangedPaths(in *gitalypb.FindChangedPathsRequest, stream gitalypb.DiffService_FindChangedPathsServer) error {
	if err := s.validateFindChangedPathsRequestParams(stream.Context(), in); err != nil {
		return err
	}

	diffChunker := chunk.New(&findChangedPathsSender{stream: stream})

	cmd, err := git.NewCommand(stream.Context(), in.Repository, nil, git.SubCmd{
		Name: "diff-tree",
		Flags: []git.Option{
			git.Flag{Name: "-z"},
			git.Flag{Name: "--stdin"},
			git.Flag{Name: "-m"},
			git.Flag{Name: "-r"},
			git.Flag{Name: "--name-status"},
			git.Flag{Name: "--no-renames"},
			git.Flag{Name: "--no-commit-id"},
			git.Flag{Name: "--diff-filter=AMDTC"},
		},
	}, git.WithStdin(strings.NewReader(strings.Join(in.GetCommits(), "\n")+"\n")))
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return fmt.Errorf("FindChangedPaths Stdin Err: %w", err)
		}
		return status.Errorf(codes.Internal, "FindChangedPaths: Cmd Err: %v", err)
	}

	if err := parsePaths(bufio.NewReader(cmd), diffChunker); err != nil {
		return fmt.Errorf("FindChangedPaths Parsing Err: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Unavailable, "FindChangedPaths: Cmd Wait Err: %v", err)
	}

	return diffChunker.Flush()
}

func parsePaths(reader *bufio.Reader, chunker *chunk.Chunker) error {
	for {
		path, err := nextPath(reader)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("FindChangedPaths Next Path Err: %w", err)
		}

		if err := chunker.Send(path); err != nil {
			return fmt.Errorf("FindChangedPaths: err sending to chunker: %v", err)
		}
	}

	return nil
}

func nextPath(reader *bufio.Reader) (*gitalypb.ChangedPaths, error) {
	pathStatus, err := reader.ReadBytes(numStatDelimiter)
	if err != nil {
		return nil, err
	}

	path, err := reader.ReadBytes(numStatDelimiter)
	if err != nil {
		return nil, err
	}

	statusTypeMap := map[string]gitalypb.ChangedPaths_Status{
		"M": gitalypb.ChangedPaths_MODIFIED,
		"D": gitalypb.ChangedPaths_DELETED,
		"T": gitalypb.ChangedPaths_TYPE_CHANGE,
		"C": gitalypb.ChangedPaths_COPIED,
		"A": gitalypb.ChangedPaths_ADDED,
	}

	parsedPath, ok := statusTypeMap[string(pathStatus[:len(pathStatus)-1])]
	if !ok {
		return nil, status.Errorf(codes.Internal, "FindChangedPaths: Unknown changed paths returned: %v", string(pathStatus))
	}

	changedPath := &gitalypb.ChangedPaths{
		Status: parsedPath,
		Path:   path[:len(path)-1],
	}

	return changedPath, nil
}

// This sender implements the interface in the chunker class
type findChangedPathsSender struct {
	paths  []*gitalypb.ChangedPaths
	stream gitalypb.DiffService_FindChangedPathsServer
}

func (t *findChangedPathsSender) Reset() {
	t.paths = nil
}

func (t *findChangedPathsSender) Append(m proto.Message) {
	t.paths = append(t.paths, m.(*gitalypb.ChangedPaths))
}

func (t *findChangedPathsSender) Send() error {
	return t.stream.Send(&gitalypb.FindChangedPathsResponse{
		Paths: t.paths,
	})
}

func (s *server) validateFindChangedPathsRequestParams(ctx context.Context, in *gitalypb.FindChangedPathsRequest) error {
	repo := in.GetRepository()
	if _, err := s.locator.GetRepoPath(repo); err != nil {
		return err
	}

	gitRepo := git.NewRepository(repo, config.Config)

	for _, commit := range in.GetCommits() {
		if commit == "" {
			return status.Errorf(codes.InvalidArgument, "FindChangedPaths: commits cannot contain an empty commit")
		}

		containsRef, err := gitRepo.HasRevision(ctx, git.Revision(commit+"^{commit}"))
		if err != nil {
			return fmt.Errorf("contains ref err: %w", err)
		}

		if !containsRef {
			return status.Errorf(codes.NotFound, "FindChangedPaths: commit: %v can not be found", commit)
		}
	}

	return nil
}
