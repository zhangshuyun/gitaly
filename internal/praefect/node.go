package praefect

import (
	"context"

	"google.golang.org/grpc"
)

// Node is a backend Gitaly node that is responsible for hosting repositories
// in a specific storage location
type Node struct {
	storage string // storage location ID (e.g. default)
	cc      *grpc.ClientConn
}

// PullReplication will attempt to replicate changes from a primary replica
func (n Node) PullReplication(ctx context.Context, primary Repository) error {
	// TODO: replication logic in #1484
	return nil
}
