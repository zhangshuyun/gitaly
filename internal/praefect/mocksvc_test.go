package praefect

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/mock"
)

type (
	repoAccessorUnaryFunc func(context.Context, *mock.RepoRequest) (*empty.Empty, error)
	repoMutatorUnaryFunc  func(context.Context, *mock.RepoRequest) (*empty.Empty, error)
)

// mockSvc is an implementation of mock.SimpleServer for testing purposes. The
// gRPC stub can be updated by running `make proto`.
type mockSvc struct {
	mock.UnimplementedSimpleServiceServer
	repoAccessorUnary repoAccessorUnaryFunc
	repoMutatorUnary  repoMutatorUnaryFunc
}

// RepoAccessorUnary is implemented by a callback
func (m *mockSvc) RepoAccessorUnary(ctx context.Context, req *mock.RepoRequest) (*empty.Empty, error) {
	return m.repoAccessorUnary(ctx, req)
}

// RepoMutatorUnary is implemented by a callback
func (m *mockSvc) RepoMutatorUnary(ctx context.Context, req *mock.RepoRequest) (*empty.Empty, error) {
	return m.repoMutatorUnary(ctx, req)
}
