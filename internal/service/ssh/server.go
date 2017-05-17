package ssh

import pb "gitlab.com/gitlab-org/gitaly-proto/go"

type server struct{}

// NewServer creates a new instance of a grpc SSHServer
func NewServer() pb.SSHServer {
	return &server{}
}
