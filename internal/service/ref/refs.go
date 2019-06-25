package ref

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	gitlog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
)

var (
	master = []byte("refs/heads/master")

	// We declare the following functions in variables so that we can override them in our tests
	headReference = _headReference
	// FindBranchNames is exported to be used in other packages
	FindBranchNames = _findBranchNames
)

type findRefsOpts struct {
	cmdArgs []string
	delim   []byte
}

func findRefs(ctx context.Context, writer lines.Sender, repo *gitalypb.Repository, patterns []string, opts *findRefsOpts) error {
	baseArgs := []string{"for-each-ref"}

	var args []string
	if len(opts.cmdArgs) == 0 {
		args = append(baseArgs, "--format=%(refname)") // Default format
	} else {
		args = append(baseArgs, opts.cmdArgs...)
	}

	args = append(args, patterns...)
	cmd, err := git.Command(ctx, repo, args...)
	if err != nil {
		return err
	}

	if err := lines.Send(cmd, writer, opts.delim); err != nil {
		return err
	}

	return cmd.Wait()
}

type tagSender struct {
	tags   []*gitalypb.Tag
	stream gitalypb.RefService_FindAllTagsServer
}

func (t *tagSender) Reset() {
	t.tags = nil
}

func (t *tagSender) Append(i chunk.Item) {
	t.tags = append(t.tags, i.(*gitalypb.Tag))
}

func (t *tagSender) Send() error {
	return t.stream.Send(&gitalypb.FindAllTagsResponse{
		Tags: t.tags,
	})
}

func parseAndReturnTags(ctx context.Context, repo *gitalypb.Repository, stream gitalypb.RefService_FindAllTagsServer) error {
	tagsCmd, err := git.Command(ctx, repo, "for-each-ref", "--format", "%(objectname) %(objecttype) %(refname:lstrip=2)", "refs/tags/")
	if err != nil {
		return fmt.Errorf("for-each-ref error: %v", err)
	}

	c, err := catfile.New(ctx, repo)
	if err != nil {
		return fmt.Errorf("error creating catfile: %v", err)
	}

	tagChunker := chunk.New(&tagSender{stream: stream})

	scanner := bufio.NewScanner(tagsCmd)
	for scanner.Scan() {
		fields := strings.SplitN(scanner.Text(), " ", 3)
		if len(fields) != 3 {
			return fmt.Errorf("invalid output from for-each-ref command: %v", scanner.Text())
		}

		tagID, refType, refName := fields[0], fields[1], fields[2]

		tag := &gitalypb.Tag{
			Id:   tagID,
			Name: []byte(refName),
		}
		switch refType {
		// annotated tag
		case "tag":
			tag, err = gitlog.GetTagCatfile(c, tagID, refName)
			if err != nil {
				return fmt.Errorf("getting annotated tag: %v", err)
			}
		case "commit":
			commit, err := gitlog.GetCommitCatfile(c, tagID)
			if err != nil {
				return fmt.Errorf("getting commit catfile: %v", err)
			}
			tag.TargetCommit = commit
		}

		if err := tagChunker.Send(tag); err != nil {
			return fmt.Errorf("sending to chunker: %v", err)
		}
	}

	if err := tagsCmd.Wait(); err != nil {
		return fmt.Errorf("tag command: %v", err)
	}

	if err := tagChunker.Flush(); err != nil {
		return fmt.Errorf("flushing chunker: %v", err)
	}

	return nil
}

func (s *server) FindAllTags(in *gitalypb.FindAllTagsRequest, stream gitalypb.RefService_FindAllTagsServer) error {
	ctx := stream.Context()

	if err := validateFindAllTagsRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := parseAndReturnTags(ctx, in.GetRepository(), stream); err != nil {
		return helper.ErrInternal(err)
	}
	return nil
}

func (s *server) FindRef(ctx context.Context, req *gitalypb.FindRefRequest) (*gitalypb.FindRefResponse, error) {
	refPath := string(req.GetRefPath())
	if len(refPath) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Ref path cannot be empty")
	}
	repo := req.GetRepository()

	cmd, err := git.Command(ctx, repo, "for-each-ref", "--format", "%(objectname)", refPath)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(cmd)
	revision, _, err := reader.ReadLine()
	if err != nil {
		if err == io.EOF {
			return &gitalypb.FindRefResponse{}, nil
		}
		return nil, err
	}

	commit, err := log.GetCommit(ctx, repo, string(revision))
	if err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return &gitalypb.FindRefResponse{
		Ref: &gitalypb.Ref{
			RefPath:         []byte(refPath),
			TargetCommit: commit,
		},
	}, nil
}

func validateFindAllTagsRequest(request *gitalypb.FindAllTagsRequest) error {
	if request.GetRepository() == nil {
		return errors.New("empty Repository")
	}

	if _, err := helper.GetRepoPath(request.GetRepository()); err != nil {
		return fmt.Errorf("invalid git directory: %v", err)
	}

	return nil
}

func _findBranchNames(ctx context.Context, repo *gitalypb.Repository) ([][]byte, error) {
	var names [][]byte

	cmd, err := git.Command(ctx, repo, "for-each-ref", "refs/heads", "--format=%(refname)")
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		names = lines.CopyAndAppend(names, scanner.Bytes())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading standard input: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return names, nil
}

func _headReference(ctx context.Context, repo *gitalypb.Repository) ([]byte, error) {
	var headRef []byte

	cmd, err := git.Command(ctx, repo, "rev-parse", "--symbolic-full-name", "HEAD")
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(cmd)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	headRef = scanner.Bytes()

	if err := cmd.Wait(); err != nil {
		// If the ref pointed at by HEAD doesn't exist, the rev-parse fails
		// returning the string `"HEAD"`, so we return `nil` without error.
		if bytes.Equal(headRef, []byte("HEAD")) {
			return nil, nil
		}

		return nil, err
	}

	return headRef, nil
}

// DefaultBranchName looks up the name of the default branch given a repoPath
func DefaultBranchName(ctx context.Context, repo *gitalypb.Repository) ([]byte, error) {
	branches, err := FindBranchNames(ctx, repo)

	if err != nil {
		return nil, err
	}

	// Return empty ref name if there are no branches
	if len(branches) == 0 {
		return nil, nil
	}

	// Return first branch name if there's only one
	if len(branches) == 1 {
		return branches[0], nil
	}

	hasMaster := false
	headRef, err := headReference(ctx, repo)
	if err != nil {
		return nil, err
	}

	for _, branch := range branches {
		// Return HEAD if it exists and corresponds to a branch
		if headRef != nil && bytes.Equal(headRef, branch) {
			return headRef, nil
		}
		if bytes.Equal(branch, master) {
			hasMaster = true
		}
	}
	// Return `ref/names/master` if it exists
	if hasMaster {
		return master, nil
	}
	// If all else fails, return the first branch name
	return branches[0], nil
}

// FindDefaultBranchName returns the default branch name for the given repository
func (s *server) FindDefaultBranchName(ctx context.Context, in *gitalypb.FindDefaultBranchNameRequest) (*gitalypb.FindDefaultBranchNameResponse, error) {
	defaultBranchName, err := DefaultBranchName(ctx, in.Repository)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.FindDefaultBranchNameResponse{Name: defaultBranchName}, nil
}

func parseSortKey(sortKey gitalypb.FindLocalBranchesRequest_SortBy) string {
	switch sortKey {
	case gitalypb.FindLocalBranchesRequest_NAME:
		return "refname"
	case gitalypb.FindLocalBranchesRequest_UPDATED_ASC:
		return "committerdate"
	case gitalypb.FindLocalBranchesRequest_UPDATED_DESC:
		return "-committerdate"
	}

	panic("never reached") // famous last words
}

// FindLocalBranches creates a stream of branches for all local branches in the given repository
func (s *server) FindLocalBranches(in *gitalypb.FindLocalBranchesRequest, stream gitalypb.RefService_FindLocalBranchesServer) error {
	if err := findLocalBranches(in, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func findLocalBranches(in *gitalypb.FindLocalBranchesRequest, stream gitalypb.RefService_FindLocalBranchesServer) error {
	ctx := stream.Context()
	c, err := catfile.New(ctx, in.Repository)
	if err != nil {
		return err
	}

	writer := newFindLocalBranchesWriter(stream, c)
	opts := &findRefsOpts{
		cmdArgs: []string{
			// %00 inserts the null character into the output (see for-each-ref docs)
			"--format=" + strings.Join(localBranchFormatFields, "%00"),
			"--sort=" + parseSortKey(in.GetSortBy()),
		},
	}

	return findRefs(ctx, writer, in.Repository, []string{"refs/heads"}, opts)
}

func (s *server) FindAllBranches(in *gitalypb.FindAllBranchesRequest, stream gitalypb.RefService_FindAllBranchesServer) error {
	if err := findAllBranches(in, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func findAllBranches(in *gitalypb.FindAllBranchesRequest, stream gitalypb.RefService_FindAllBranchesServer) error {
	args := []string{
		// %00 inserts the null character into the output (see for-each-ref docs)
		"--format=" + strings.Join(localBranchFormatFields, "%00"),
	}

	patterns := []string{"refs/heads", "refs/remotes"}

	if in.MergedOnly {
		defaultBranchName, err := DefaultBranchName(stream.Context(), in.Repository)
		if err != nil {
			return err
		}

		args = append(args, fmt.Sprintf("--merged=%s", string(defaultBranchName)))

		if len(in.MergedBranches) > 0 {
			patterns = nil

			for _, mergedBranch := range in.MergedBranches {
				patterns = append(patterns, string(mergedBranch))
			}
		}
	}

	ctx := stream.Context()
	c, err := catfile.New(ctx, in.Repository)
	if err != nil {
		return err
	}

	opts := &findRefsOpts{
		cmdArgs: args,
	}
	writer := newFindAllBranchesWriter(stream, c)

	return findRefs(ctx, writer, in.Repository, patterns, opts)
}
