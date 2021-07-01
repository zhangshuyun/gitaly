package cache

import (
	"context"
	"io"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// Cache is a stream-oriented cache which stores RPC responses keyed by protobuf requests.
type Cache interface {
	Streamer
	Invalidator
}

// Streamer abstracts away the cache concrete type so that it can be override in tests.
type Streamer interface {
	GetStream(context.Context, *gitalypb.Repository, proto.Message) (io.ReadCloser, error)
	PutStream(context.Context, *gitalypb.Repository, proto.Message, io.Reader) error
}

// Invalidator is able to invalidate parts of the cache pertinent to a
// specific repository. Before a repo mutating operation, StartLease should
// be called. Once the operation is complete, the returned LeaseEnder should
// be invoked to end the lease.
type Invalidator interface {
	StartLease(*gitalypb.Repository) (LeaseEnder, error)
}

// LeaseEnder allows the caller to indicate when a lease is no longer needed
type LeaseEnder interface {
	EndLease(context.Context) error
}
