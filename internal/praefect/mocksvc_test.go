package praefect

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
)

type simpleUnaryUnaryCallback func(context.Context, *mock.SimpleRequest) (*mock.SimpleResponse, error)

// mockSvc is an implementation of mock.SimpleServer for testing purposes. The
// gRPC stub can be updated via go generate:
//
//go:generate protoc --go_out=plugins=grpc:. -I../../proto -I./ mock/mock.proto
//go:generate goimports -w mock/mock.pb.go
type mockSvc struct {
	simpleUnaryUnary simpleUnaryUnaryCallback
}

// SimpleUnaryUnary is implemented by a callback
func (m *mockSvc) SimpleUnaryUnary(ctx context.Context, req *mock.SimpleRequest) (*mock.SimpleResponse, error) {
	return m.simpleUnaryUnary(ctx, req)
}
