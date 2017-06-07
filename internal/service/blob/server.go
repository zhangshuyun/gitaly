package blob

import pb "gitlab.com/gitlab-org/gitaly-proto/go"

const msgSizeThreshold = 8 * 1024

type server struct {
	MsgSizeThreshold int
}

// NewServer creates a new instance of a grpc BlobServer
func NewServer() pb.BlobServer {
	return &server{MsgSizeThreshold: msgSizeThreshold}
}
