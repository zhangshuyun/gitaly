package ref

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	gitlog "gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	tagFormat = "%(objectname) %(objecttype) %(refname:lstrip=2)"
)

var (
	master = []byte("refs/heads/master")

	// We declare the following functions in variables so that we can override them in our tests
	headReference = _headReference
	// FindBranchNames is exported to be used in other packages
	FindBranchNames = _findBranchNames
)

type findRefsOpts struct {
	cmdArgs []git.Option
	delim   byte
	lines.SenderOpts
}

func findRefs(ctx context.Context, writer lines.Sender, repo *gitalypb.Repository, patterns []string, opts *findRefsOpts) error {
	var options []git.Option

	if len(opts.cmdArgs) == 0 {
		options = append(options, git.Flag{Name: "--format=%(refname)"}) // Default format
	} else {
		options = append(options, opts.cmdArgs...)
	}

	cmd, err := git.NewCommand(ctx, repo, nil, git.SubCmd{
		Name:  "for-each-ref",
		Flags: options,
		Args:  patterns,
	})
	if err != nil {
		return err
	}

	if err := lines.Send(cmd, writer, lines.SenderOpts{
		IsPageToken: opts.IsPageToken,
		Delimiter:   opts.delim,
		Limit:       opts.Limit,
	}); err != nil {
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

func (t *tagSender) Append(m proto.Message) {
	t.tags = append(t.tags, m.(*gitalypb.Tag))
}

func (t *tagSender) Send() error {
	return t.stream.Send(&gitalypb.FindAllTagsResponse{
		Tags: t.tags,
	})
}

func (s *server) parseAndReturnTags(ctx context.Context, repo *gitalypb.Repository, stream gitalypb.RefService_FindAllTagsServer) error {
	tagsCmd, err := git.NewCommand(ctx, repo, nil, git.SubCmd{
		Name: "for-each-ref",
		Flags: []git.Option{
			git.ValueFlag{"--format", tagFormat},
		},
		Args: []string{"refs/tags/"},
	})
	if err != nil {
		return fmt.Errorf("for-each-ref error: %v", err)
	}

	c, err := catfile.New(ctx, s.locator, repo)
	if err != nil {
		return fmt.Errorf("error creating catfile: %v", err)
	}

	tagChunker := chunk.New(&tagSender{stream: stream})

	scanner := bufio.NewScanner(tagsCmd)
	for scanner.Scan() {
		tag, err := parseTagLine(ctx, c, scanner.Text())
		if err != nil {
			return fmt.Errorf("parsing tag: %v", err)
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

	if err := s.validateFindAllTagsRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := s.parseAndReturnTags(ctx, in.GetRepository(), stream); err != nil {
		return helper.ErrInternal(err)
	}
	return nil
}

func (s *server) validateFindAllTagsRequest(request *gitalypb.FindAllTagsRequest) error {
	if request.GetRepository() == nil {
		return errors.New("empty Repository")
	}

	if _, err := s.locator.GetRepoPath(request.GetRepository()); err != nil {
		return fmt.Errorf("invalid git directory: %v", err)
	}

	return nil
}

func _findBranchNames(ctx context.Context, repo *gitalypb.Repository) ([][]byte, error) {
	var names [][]byte

	cmd, err := git.NewCommand(ctx, repo, nil, git.SubCmd{
		Name:  "for-each-ref",
		Flags: []git.Option{git.Flag{Name: "--format=%(refname)"}},
		Args:  []string{"refs/heads"}},
	)
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

	cmd, err := git.NewCommand(ctx, repo, nil, git.SubCmd{
		Name:  "rev-parse",
		Flags: []git.Option{git.Flag{Name: "--symbolic-full-name"}},
		Args:  []string{"HEAD"},
	})
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

// SetDefaultBranchRef overwrites the default branch ref for the repository
func SetDefaultBranchRef(ctx context.Context, repo *gitalypb.Repository, ref string, cfg config.Cfg) error {
	cmd, err := git.NewCommand(ctx, repo, nil, git.SubCmd{
		Name: "symbolic-ref",
		Args: []string{"HEAD", ref},
	}, git.WithRefTxHook(ctx, repo, cfg))
	if err != nil {
		return err
	}
	return cmd.Wait()
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
	if err := s.findLocalBranches(in, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) findLocalBranches(in *gitalypb.FindLocalBranchesRequest, stream gitalypb.RefService_FindLocalBranchesServer) error {
	ctx := stream.Context()
	c, err := catfile.New(ctx, s.locator, in.Repository)
	if err != nil {
		return err
	}

	writer := newFindLocalBranchesWriter(stream, c)
	opts := paginationParamsToOpts(in.GetPaginationParams())
	opts.cmdArgs = []git.Option{
		// %00 inserts the null character into the output (see for-each-ref docs)
		git.Flag{Name: "--format=" + strings.Join(localBranchFormatFields, "%00")},
		git.Flag{Name: "--sort=" + parseSortKey(in.GetSortBy())},
	}

	return findRefs(ctx, writer, in.Repository, []string{"refs/heads"}, opts)
}

func (s *server) FindAllBranches(in *gitalypb.FindAllBranchesRequest, stream gitalypb.RefService_FindAllBranchesServer) error {
	if err := s.findAllBranches(in, stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) findAllBranches(in *gitalypb.FindAllBranchesRequest, stream gitalypb.RefService_FindAllBranchesServer) error {
	args := []git.Option{
		// %00 inserts the null character into the output (see for-each-ref docs)
		git.Flag{Name: "--format=" + strings.Join(localBranchFormatFields, "%00")},
	}

	patterns := []string{"refs/heads", "refs/remotes"}

	if in.MergedOnly {
		defaultBranchName, err := DefaultBranchName(stream.Context(), in.Repository)
		if err != nil {
			return err
		}

		args = append(args, git.Flag{fmt.Sprintf("--merged=%s", string(defaultBranchName))})

		if len(in.MergedBranches) > 0 {
			patterns = nil

			for _, mergedBranch := range in.MergedBranches {
				patterns = append(patterns, string(mergedBranch))
			}
		}
	}

	ctx := stream.Context()
	c, err := catfile.New(ctx, s.locator, in.Repository)
	if err != nil {
		return err
	}

	opts := paginationParamsToOpts(nil)
	opts.cmdArgs = args

	writer := newFindAllBranchesWriter(stream, c)

	return findRefs(ctx, writer, in.Repository, patterns, opts)
}

func (s *server) FindTag(ctx context.Context, in *gitalypb.FindTagRequest) (*gitalypb.FindTagResponse, error) {
	var err error
	if err = s.validateFindTagRequest(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	var tag *gitalypb.Tag

	if tag, err = s.findTag(ctx, in.GetRepository(), in.GetTagName()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.FindTagResponse{Tag: tag}, nil
}

// parseTagLine parses a line of text with the output format %(objectname) %(objecttype) %(refname:lstrip=2)
func parseTagLine(ctx context.Context, c catfile.Batch, tagLine string) (*gitalypb.Tag, error) {
	fields := strings.SplitN(tagLine, " ", 3)
	if len(fields) != 3 {
		return nil, fmt.Errorf("invalid output from for-each-ref command: %v", tagLine)
	}

	tagID, refType, refName := fields[0], fields[1], fields[2]

	tag := &gitalypb.Tag{
		Id:   tagID,
		Name: []byte(refName),
	}

	switch refType {
	// annotated tag
	case "tag":
		tag, err := gitlog.GetTagCatfile(ctx, c, tagID, refName, true, true)
		if err != nil {
			return nil, fmt.Errorf("getting annotated tag: %v", err)
		}
		return tag, nil
	case "commit":
		commit, err := gitlog.GetCommitCatfile(ctx, c, tagID)
		if err != nil {
			return nil, fmt.Errorf("getting commit catfile: %v", err)
		}
		tag.TargetCommit = commit
		return tag, nil
	default:
		return tag, nil
	}
}

func (s *server) findTag(ctx context.Context, repository *gitalypb.Repository, tagName []byte) (*gitalypb.Tag, error) {
	tagCmd, err := git.NewCommand(ctx, repository, nil,
		git.SubCmd{
			Name: "tag",
			Flags: []git.Option{
				git.Flag{Name: "-l"}, git.ValueFlag{"--format", tagFormat},
			},
			Args: []string{string(tagName)},
		},
		git.WithRefTxHook(ctx, repository, config.Config),
	)
	if err != nil {
		return nil, fmt.Errorf("for-each-ref error: %v", err)
	}

	c, err := catfile.New(ctx, s.locator, repository)
	if err != nil {
		return nil, err
	}

	var tag *gitalypb.Tag

	scanner := bufio.NewScanner(tagCmd)
	if scanner.Scan() {
		tag, err = parseTagLine(ctx, c, scanner.Text())
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("no tag found")
	}

	if err = tagCmd.Wait(); err != nil {
		return nil, err
	}

	return tag, nil
}

func (s *server) validateFindTagRequest(in *gitalypb.FindTagRequest) error {
	if in.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	if _, err := s.locator.GetRepoPath(in.GetRepository()); err != nil {
		return fmt.Errorf("invalid git directory: %v", err)
	}

	if in.GetTagName() == nil {
		return errors.New("tag name is empty")
	}
	return nil
}

func paginationParamsToOpts(p *gitalypb.PaginationParameter) *findRefsOpts {
	opts := &findRefsOpts{delim: '\n'}
	opts.IsPageToken = func(_ []byte) bool { return true }
	opts.Limit = math.MaxInt32

	if p == nil {
		return opts
	}

	if p.GetLimit() >= 0 {
		opts.Limit = int(p.GetLimit())
	}

	if p.GetPageToken() != "" {
		opts.IsPageToken = func(l []byte) bool { return bytes.Compare(l, []byte(p.GetPageToken())) >= 0 }
	}

	return opts
}
