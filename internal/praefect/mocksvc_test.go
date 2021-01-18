package praefect

import (
	"context"

	"github.com/golang/protobuf/ptypes/empty"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
)

type (
	serverAccessorFunc    func(context.Context, *mock.SimpleRequest) (*mock.SimpleResponse, error)
	repoAccessorUnaryFunc func(context.Context, *mock.RepoRequest) (*empty.Empty, error)
	repoMutatorUnaryFunc  func(context.Context, *mock.RepoRequest) (*empty.Empty, error)
)

// mockSvc is an implementation of mock.SimpleServer for testing purposes. The
// gRPC stub can be updated by running `make proto`.
type mockSvc struct {
	serverAccessor    serverAccessorFunc
	repoAccessorUnary repoAccessorUnaryFunc
	repoMutatorUnary  repoMutatorUnaryFunc
}

// ServerAccessor is implemented by a callback
func (m *mockSvc) ServerAccessor(ctx context.Context, req *mock.SimpleRequest) (*mock.SimpleResponse, error) {
	return m.serverAccessor(ctx, req)
}

// RepoAccessorUnary is implemented by a callback
func (m *mockSvc) RepoAccessorUnary(ctx context.Context, req *mock.RepoRequest) (*empty.Empty, error) {
	return m.repoAccessorUnary(ctx, req)
}

// RepoMutatorUnary is implemented by a callback
func (m *mockSvc) RepoMutatorUnary(ctx context.Context, req *mock.RepoRequest) (*empty.Empty, error) {
	return m.repoMutatorUnary(ctx, req)
}
