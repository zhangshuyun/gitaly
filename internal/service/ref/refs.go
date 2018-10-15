package ref

import (
	"bufio"
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"Patterns": patterns,
	}).Debug("FindRefs")

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

// FindAllBranchNames creates a stream of ref names for all branches in the given repository
func (s *server) FindAllBranchNames(in *gitalypb.FindAllBranchNamesRequest, stream gitalypb.RefService_FindAllBranchNamesServer) error {
	return findRefs(stream.Context(), newFindAllBranchNamesWriter(stream), in.Repository, []string{"refs/heads"}, &findRefsOpts{})
}

// FindAllTagNames creates a stream of ref names for all tags in the given repository
func (s *server) FindAllTagNames(in *gitalypb.FindAllTagNamesRequest, stream gitalypb.RefService_FindAllTagNamesServer) error {
	return findRefs(stream.Context(), newFindAllTagNamesWriter(stream), in.Repository, []string{"refs/tags"}, &findRefsOpts{})
}

func (s *server) FindAllTags(in *gitalypb.FindAllTagsRequest, stream gitalypb.RefService_FindAllTagsServer) error {
	ctx := stream.Context()

	client, err := s.RefServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.FindAllTags(clientCtx, in)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
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
	grpc_logrus.Extract(ctx).Debug("FindDefaultBranchName")

	defaultBranchName, err := DefaultBranchName(ctx, in.Repository)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.Internal, err.Error())
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
	args := []string{
		// %00 inserts the null character into the output (see for-each-ref docs)
		"--format=" + strings.Join(localBranchFormatFields, "%00"),
	}

	patterns := []string{"refs/heads", "refs/remotes"}

	if in.MergedOnly {
		defaultBranchName, err := DefaultBranchName(stream.Context(), in.Repository)
		if err != nil {
			if _, ok := status.FromError(err); ok {
				return err
			}
			return status.Errorf(codes.Internal, err.Error())
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

// ListBranchNamesContainingCommit returns a maximum of in.GetLimit() Branch names
// which contain the SHA1 passed as argument
func (*server) ListBranchNamesContainingCommit(in *gitalypb.ListBranchNamesContainingCommitRequest, stream gitalypb.RefService_ListBranchNamesContainingCommitServer) error {
	if !validCommitID(in.GetCommitId()) {
		return status.Errorf(codes.InvalidArgument, "commit id was not a 40 character hexidecimal")
	}

	args := []string{fmt.Sprintf("--contains=%s", in.GetCommitId()), "--format=%(refname:strip=2)"}
	if in.GetLimit() != 0 {
		args = append(args, fmt.Sprintf("--count=%d", in.GetLimit()))
	}

	writer := func(refs [][]byte) error {
		return stream.Send(&gitalypb.ListBranchNamesContainingCommitResponse{BranchNames: refs})
	}

	return findRefs(stream.Context(), writer, in.Repository, []string{"refs/heads"},
		&findRefsOpts{
			cmdArgs: args,
			delim:   []byte("\n"),
		})
}

// ListTagNamesContainingCommit returns a maximum of in.GetLimit() Tag names
// which contain the SHA1 passed as argument
func (*server) ListTagNamesContainingCommit(in *gitalypb.ListTagNamesContainingCommitRequest, stream gitalypb.RefService_ListTagNamesContainingCommitServer) error {
	if !validCommitID(in.GetCommitId()) {
		return status.Errorf(codes.InvalidArgument, "commit id was not a 40 character hexidecimal")
	}

	args := []string{fmt.Sprintf("--contains=%s", in.GetCommitId()), "--format=%(refname:strip=2)"}
	if in.GetLimit() != 0 {
		args = append(args, fmt.Sprintf("--count=%d", in.GetLimit()))
	}

	writer := func(refs [][]byte) error {
		return stream.Send(&gitalypb.ListTagNamesContainingCommitResponse{TagNames: refs})
	}

	return findRefs(stream.Context(), writer, in.Repository, []string{"refs/tags"},
		&findRefsOpts{
			cmdArgs: args,
			delim:   []byte("\n"),
		})
}

func validCommitID(id string) bool {
	if match, err := regexp.MatchString(`\A[0-9a-f]{40}\z`, id); !match || err != nil {
		return false
	}

	return true
}
