package remoterepo

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// Repository represents a Git repository on a different Gitaly storage
type Repository struct {
	repo *gitalypb.Repository
	conn *grpc.ClientConn
}

// New creates a new remote Repository from its protobuf representation.
func New(ctx context.Context, repo *gitalypb.Repository, pool *client.Pool) (Repository, error) {
	server, err := helper.ExtractGitalyServer(ctx, repo.GetStorageName())
	if err != nil {
		return Repository{}, fmt.Errorf("remote repository: %w", err)
	}

	cc, err := pool.Dial(ctx, server.Address, server.Token)
	if err != nil {
		return Repository{}, fmt.Errorf("dial: %w", err)
	}

	return Repository{repo: repo, conn: cc}, nil
}

// ResolveRevision will dial to the remote repository and attempt to resolve the
// revision string via the gRPC interface.
func (rr Repository) ResolveRevision(ctx context.Context, revision git.Revision) (git.ObjectID, error) {
	cli := gitalypb.NewCommitServiceClient(rr.conn)
	resp, err := cli.FindCommit(ctx, &gitalypb.FindCommitRequest{
		Repository: rr.repo,
		Revision:   []byte(revision.String()),
	})
	if err != nil {
		return "", err
	}

	oidHex := resp.GetCommit().GetId()
	if oidHex == "" {
		return "", git.ErrReferenceNotFound
	}

	oid, err := git.NewObjectIDFromHex(oidHex)
	if err != nil {
		return "", err
	}

	return oid, nil
}

func (rr Repository) HasBranches(ctx context.Context) (bool, error) {
	resp, err := gitalypb.NewRepositoryServiceClient(rr.conn).HasLocalBranches(
		ctx, &gitalypb.HasLocalBranchesRequest{Repository: rr.repo})
	if err != nil {
		return false, fmt.Errorf("has local branches: %w", err)
	}

	return resp.Value, nil
}
