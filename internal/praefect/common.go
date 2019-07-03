package praefect

import "google.golang.org/grpc"

// Repository provides all necessary information to address a repository hosted
// in a specific Gitaly replica
type Repository struct {
	RelativePath string // relative path of repository
	Storage      string // storage location, e.g. default
}

// Node is a wrapper around the grpc client connection for a backend Gitaly node
type Node struct {
	Storage string
	Address string
	Token   string
	cc      *grpc.ClientConn
}

// logging keys to use with logrus WithField
const (
	logKeyProjectPath = "ProjectPath"
)
