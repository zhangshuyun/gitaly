package blob

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/helper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type blobSender func(size int64, oid string, data []byte) error

// GetBlob might get depricated in favour of the more versatile GetBlobs.
func (s *server) GetBlob(in *pb.GetBlobRequest, stream pb.BlobService_GetBlobServer) error {
	if err := validateRequest(in); err != nil {
		return grpc.Errorf(codes.InvalidArgument, "GetBlob: %v", err)
	}

	repoPath, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}

	return getBlobs(stream.Context(), repoPath, []string{in.Oid}, in.Limit, func(size int64, oid string, data []byte) error {
		resp := &pb.GetBlobResponse{
			Size: size,
			Oid:  oid,
			Data: data,
		}

		return stream.Send(resp)
	})
}

func validateRequest(in *pb.GetBlobRequest) error {
	if len(in.GetOid()) == 0 {
		return fmt.Errorf("empty Oid")
	}
	return nil
}
