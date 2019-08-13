package commit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const commitsPerPage int = 20

func (s *server) FindCommits(req *gitalypb.FindCommitsRequest, stream gitalypb.CommitService_FindCommitsServer) error {
	ctx := stream.Context()

	if err := git.ValidateRevisionAllowEmpty(req.Revision); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	// Use Gitaly's default branch lookup function because that is already
	// migrated.
	if revision := req.Revision; len(revision) == 0 && !req.GetAll() {
		var err error
		req.Revision, err = defaultBranchName(ctx, req.Repository)
		if err != nil {
			return helper.ErrInternal(fmt.Errorf("defaultBranchName: %v", err))
		}
	}
	// Clients might send empty paths. That is an error
	for _, path := range req.Paths {
		if len(path) == 0 {
			return helper.ErrInvalidArgument(errors.New("path is empty string"))
		}
	}

	if err := findCommits(ctx, req, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func findCommits(ctx context.Context, req *gitalypb.FindCommitsRequest, stream gitalypb.CommitService_FindCommitsServer) error {

	args := getLogCommandFlags(req)
	logCmd, err := git.Command(ctx, req.GetRepository(), args...)
	if err != nil {
		return fmt.Errorf("error when creating git log command: %v", err)
	}

	batch, err := catfile.New(ctx, req.GetRepository())
	if err != nil {
		return fmt.Errorf("creating catfile: %v", err)
	}

	getCommits := NewGetCommits(logCmd, batch)

	if calculateOffsetManually(req) {
		getCommits.Offset(int(req.GetOffset()))
	}

	if err := streamPaginatedCommits(getCommits, commitsPerPage, stream); err != nil {
		return fmt.Errorf("error streaming commits: %v", err)
	}
	return nil
}

func calculateOffsetManually(req *gitalypb.FindCommitsRequest) bool {
	return req.GetFollow() && req.GetOffset() > 0
}

// GetCommits wraps a git log command that can be interated on to get individual commit objects
type GetCommits struct {
	scanner *bufio.Scanner
	batch   *catfile.Batch
}

// NewGetCommits returns a new GetCommits object
func NewGetCommits(cmd *command.Command, batch *catfile.Batch) *GetCommits {
	return &GetCommits{
		scanner: bufio.NewScanner(cmd),
		batch:   batch,
	}
}

// Scan indicates whether or not there are more commits to return
func (g *GetCommits) Scan() bool {
	return g.scanner.Scan()
}

// Err returns the first non EOF error
func (g *GetCommits) Err() error {
	return g.scanner.Err()
}

// Offset skips over a number of commits
func (g *GetCommits) Offset(offset int) error {
	for i := 0; i < offset; i++ {
		if !g.Scan() {
			return fmt.Errorf("offset %d is invalid: %v", offset, g.scanner.Err())
		}
	}
	return nil
}

// Commit returns the current commit
func (g *GetCommits) Commit() (*gitalypb.GitCommit, error) {
	revision := strings.TrimSpace(g.scanner.Text())
	commit, err := log.GetCommitCatfile(g.batch, revision)
	if err != nil {
		return nil, fmt.Errorf("cat-file get commit %q: %v", revision, err)
	}
	return commit, nil
}

type findCommitsSender struct {
	stream  gitalypb.CommitService_FindCommitsServer
	commits []*gitalypb.GitCommit
}

func (s *findCommitsSender) Reset() { s.commits = nil }
func (s *findCommitsSender) Append(it chunk.Item) {
	s.commits = append(s.commits, it.(*gitalypb.GitCommit))
}
func (s *findCommitsSender) Send() error {
	return s.stream.Send(&gitalypb.FindCommitsResponse{Commits: s.commits})
}

func streamPaginatedCommits(getCommits *GetCommits, commitsPerPage int, stream gitalypb.CommitService_FindCommitsServer) error {
	chunker := chunk.New(&findCommitsSender{stream: stream})

	for getCommits.Scan() {
		commit, err := getCommits.Commit()
		if err != nil {
			return err
		}

		if err := chunker.Send(commit); err != nil {
			return err
		}
	}
	if getCommits.Err() != nil {
		return fmt.Errorf("get commits: %v", getCommits.Err())
	}

	return chunker.Flush()
}

func getLogCommandFlags(req *gitalypb.FindCommitsRequest) []string {
	args := []string{"log", "--format=format:%H"}

	//  We will perform the offset in Go because --follow doesn't play well with --skip.
	//  See: https://gitlab.com/gitlab-org/gitlab-ce/issues/3574#note_3040520
	if req.GetOffset() > 0 && !calculateOffsetManually(req) {
		args = append(args, fmt.Sprintf("--skip=%d", req.GetOffset()))
	}
	limit := req.GetLimit()
	if calculateOffsetManually(req) {
		limit += req.GetOffset()
	}
	args = append(args, fmt.Sprintf("--max-count=%d", limit))

	if req.GetFollow() && len(req.GetPaths()) > 0 {
		args = append(args, "--follow")
	}
	if req.GetSkipMerges() {
		args = append(args, "--no-merges")
	}
	if req.GetBefore() != nil {
		args = append(args, fmt.Sprintf("--before=%s", req.GetBefore().String()))
	}
	if req.GetAfter() != nil {
		args = append(args, fmt.Sprintf("--after=%s", req.GetAfter().String()))
	}
	if req.GetAll() {
		args = append(args, "--all", "--reverse")
	}
	if req.GetRevision() != nil {
		args = append(args, string(req.GetRevision()))
	}
	if len(req.GetPaths()) > 0 {
		args = append(args, "--")
		for _, path := range req.GetPaths() {
			args = append(args, string(path))
		}
	}
	return args
}
