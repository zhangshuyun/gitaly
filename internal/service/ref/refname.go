package ref

import (
	"bufio"
	"log"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"golang.org/x/net/context"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

// FindRefName returns a ref that starts with the given prefix, if one exists.
//  If there is more than one such ref there is no guarantee which one is
//  returned or that the same one is returned on each call.
func (s *server) FindRefName(ctx context.Context, in *pb.FindRefNameRequest) (*pb.FindRefNameResponse, error) {
	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return nil, err
	}
	if in.CommitId == "" {
		message := "Bad Request (empty commit sha)"
		log.Printf("FindRefName: %q", message)
		return nil, grpc.Errorf(codes.InvalidArgument, message)
	}

	ref, err := findRefName(ctx, repoPath, in.CommitId, string(in.Prefix))
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, err.Error())
	}

	return &pb.FindRefNameResponse{Name: []byte(ref)}, nil
}

// We assume `path` and `commitID` and `prefix` are non-empty
func findRefName(ctx context.Context, path, commitID, prefix string) (string, error) {
	cmd, err := helper.GitCommandReader(ctx, "--git-dir", path, "for-each-ref", "--format=%(refname)", "--count=1", prefix, "--contains", commitID)
	if err != nil {
		return "", err
	}
	defer cmd.Kill()

	log.Printf("findRefName: RepoPath=%q commitSha=%s prefix=%s", path, commitID, prefix)

	scanner := bufio.NewScanner(cmd)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return "", err
	}
	refName := scanner.Text()

	if err := cmd.Wait(); err != nil {
		// We're suppressing the error since invalid commits isn't an error
		//  according to Rails
		return "", nil
	}

	// Trailing spaces are not allowed per the documentation
	//  https://www.kernel.org/pub/software/scm/git/docs/git-check-ref-format.html
	return strings.TrimSpace(refName), nil
}
