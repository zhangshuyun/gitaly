package praefect

import "google.golang.org/grpc"

// Node is a wrapper around the grpc client connection for a backend Gitaly node
type Node struct {
	Address string
	cc      *grpc.ClientConn
}

// logging keys to use with logrus WithField
const (
	logKeyProjectPath = "ProjectPath"
)
