package ref

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
	"golang.org/x/net/context"
)

var (
	master = []byte("refs/heads/master")

	// We declare the following functions in variables so that we can override them in our tests
	headReference = _headReference
	// FindBranchNames is exported to be used in other packages
	FindBranchNames = _findBranchNames
)

type findRefsOpts struct {
	cmdArgs  []string
	splitter bufio.SplitFunc
}

func findRefs(ctx context.Context, writer lines.Sender, repo *pb.Repository, patterns []string, opts *findRefsOpts) error {
	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"Patterns": patterns,
	}).Debug("FindRefs")

	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return err
	}

	baseArgs := []string{"--git-dir", repoPath, "for-each-ref"}

	var args []string
	if len(opts.cmdArgs) == 0 {
		args = append(baseArgs, "--format=%(refname)") // Default format
	} else {
		args = append(baseArgs, opts.cmdArgs...)
	}

	args = append(args, patterns...)
	cmd, err := helper.GitCommandReader(ctx, args...)
	if err != nil {
		return err
	}
	defer cmd.CleanUpProcessGroup(ctx)

	if err := lines.Send(cmd, writer, opts.splitter); err != nil {
		return err
	}

	return cmd.Wait()
}

// FindAllBranchNames creates a stream of ref names for all branches in the given repository
func (s *server) FindAllBranchNames(in *pb.FindAllBranchNamesRequest, stream pb.RefService_FindAllBranchNamesServer) error {
	return findRefs(stream.Context(), newFindAllBranchNamesWriter(stream), in.Repository, []string{"refs/heads"}, &findRefsOpts{})
}

// FindAllTagNames creates a stream of ref names for all tags in the given repository
func (s *server) FindAllTagNames(in *pb.FindAllTagNamesRequest, stream pb.RefService_FindAllTagNamesServer) error {
	return findRefs(stream.Context(), newFindAllTagNamesWriter(stream), in.Repository, []string{"refs/tags"}, &findRefsOpts{})
}

func (s *server) FindAllTags(in *pb.FindAllTagsRequest, stream pb.RefService_FindAllTagsServer) error {
	opts := &findRefsOpts{
		cmdArgs:  []string{"--format=" + strings.Join(tagsFormatFields, "%1f") + "%00"},
		splitter: lines.ScanWithDelimiter([]byte{'\x00', '\n'}),
	}

	repo := in.GetRepository()
	return findRefs(stream.Context(), newFindAllTagsWriter(repo, stream), repo, []string{"refs/tags"}, opts)
}

func _findBranchNames(ctx context.Context, repoPath string) ([][]byte, error) {
	var names [][]byte

	cmd, err := helper.GitCommandReader(ctx, "--git-dir", repoPath, "for-each-ref", "refs/heads", "--format=%(refname)")
	if err != nil {
		return nil, err
	}
	defer cmd.CleanUpProcessGroup(ctx)

	scanner := bufio.NewScanner(cmd)
	for scanner.Scan() {
		names, _ = lines.CopyAndAppend(names, scanner.Bytes())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading standard input: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return names, nil
}

func _headReference(ctx context.Context, repoPath string) ([]byte, error) {
	var headRef []byte

	cmd, err := helper.GitCommandReader(ctx, "--git-dir", repoPath, "rev-parse", "--symbolic-full-name", "HEAD")
	if err != nil {
		return nil, err
	}
	defer cmd.CleanUpProcessGroup(ctx)

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
func DefaultBranchName(ctx context.Context, repoPath string) ([]byte, error) {
	branches, err := FindBranchNames(ctx, repoPath)

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
	headRef, err := headReference(ctx, repoPath)
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
func (s *server) FindDefaultBranchName(ctx context.Context, in *pb.FindDefaultBranchNameRequest) (*pb.FindDefaultBranchNameResponse, error) {
	grpc_logrus.Extract(ctx).Debug("FindDefaultBranchName")

	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return nil, err
	}

	defaultBranchName, err := DefaultBranchName(ctx, repoPath)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}

	return &pb.FindDefaultBranchNameResponse{Name: defaultBranchName}, nil
}

func parseSortKey(sortKey pb.FindLocalBranchesRequest_SortBy) string {
	switch sortKey {
	case pb.FindLocalBranchesRequest_NAME:
		return "refname"
	case pb.FindLocalBranchesRequest_UPDATED_ASC:
		return "committerdate"
	case pb.FindLocalBranchesRequest_UPDATED_DESC:
		return "-committerdate"
	}

	panic("never reached") // famous last words
}

// FindLocalBranches creates a stream of branches for all local branches in the given repository
func (s *server) FindLocalBranches(in *pb.FindLocalBranchesRequest, stream pb.RefService_FindLocalBranchesServer) error {
	writer := newFindLocalBranchesWriter(stream)
	opts := &findRefsOpts{
		cmdArgs: []string{
			// %00 inserts the null character into the output (see for-each-ref docs)
			"--format=" + strings.Join(localBranchFormatFields, "%00"),
			"--sort=" + parseSortKey(in.GetSortBy()),
		},
	}

	return findRefs(stream.Context(), writer, in.Repository, []string{"refs/heads"}, opts)
}

func (s *server) FindAllBranches(in *pb.FindAllBranchesRequest, stream pb.RefService_FindAllBranchesServer) error {
	opts := &findRefsOpts{
		cmdArgs: []string{
			// %00 inserts the null character into the output (see for-each-ref docs)
			"--format=" + strings.Join(localBranchFormatFields, "%00"),
		},
	}
	writer := newFindAllBranchesWriter(stream)

	return findRefs(stream.Context(), writer, in.Repository, []string{"refs/heads", "refs/remotes"}, opts)
}
