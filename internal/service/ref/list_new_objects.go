package ref

import (
	"bufio"
	"regexp"
	"strings"
	"fmt"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) ListNewObjects(in *pb.ListNewObjectsRequest, stream pb.RefService_ListNewObjectsServer) error {
	oid := in.GetCommitId()
	if match, err := regexp.MatchString(`\A[0-9a-f]{40}\z`, oid); !match || err != nil {
		return status.Errorf(codes.InvalidArgument, "commit id shoud have 40 hexidecimal characters")
	}

	fmt.Printf("The VALUE of limit is: %d", in.limit)

	ctx := stream.Context()
	revList, err := git.Command(ctx, in.GetRepository(), "rev-list", oid, "--objects", "--all")
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "ListNewObjects: gitCommand: %v", err)
	}

	batch, err := catfile.New(ctx, in.GetRepository())
	if err != nil {
		return status.Errorf(codes.Internal, "ListNewObjects: catfile: %v", err)
	}

	i := 0
	var newBlobs []*pb.NewBlobObject
	scanner := bufio.NewScanner(revList)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)

		if len(parts) == 2 {
			path := []byte(parts[1])
			info, err := batch.Info(parts[0])
			if err != nil {
				return status.Errorf(codes.Internal, "ListNewObjects: catfile: %v", err)
			}

			if info.Type == "blob" {
				newBlobs = append(newBlobs, &pb.NewBlobObject{Oid: info.Oid, Size: info.Size, Path: path,})
			}
		}

		if i%10 == 0 {
			response := &pb.ListNewObjectsResponse{NewBlobObjects: newBlobs}
			stream.Send(response)
			newBlobs = newBlobs[:0]
		}
	}

	response := &pb.ListNewObjectsResponse{NewBlobObjects: newBlobs}
	stream.Send(response)

	return revList.Wait()
}
