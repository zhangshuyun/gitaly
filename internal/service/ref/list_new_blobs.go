package ref

import (
	"bufio"
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
)

func (s *server) ListNewBlobs(in *gitalypb.ListNewBlobsRequest, stream gitalypb.RefService_ListNewBlobsServer) error {
	oid := in.GetCommitId()
	if err := validateCommitID(oid); err != nil {
		return err
	}

	ctx := stream.Context()
	cmdArgs := []string{"rev-list", oid, "--objects", "--not", "--all"}

	if in.Limit > 0 {
		cmdArgs = append(cmdArgs, "--max-count", fmt.Sprint(in.Limit))
	}

	revList, err := git.Command(ctx, in.GetRepository(), cmdArgs...)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Errorf(codes.Internal, "ListNewBlobs: gitCommand: %v", err)
	}

	batch, err := catfile.New(ctx, in.GetRepository())
	if err != nil {
		return status.Errorf(codes.Internal, "ListNewBlobs: catfile: %v", err)
	}

	var newBlobs []*gitalypb.NewBlobObject
	scanner := bufio.NewScanner(revList)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 2)

		if len(parts) != 2 {
			continue
		}

		info, err := batch.Info(parts[0])
		if err != nil {
			return status.Errorf(codes.Internal, "ListNewBlobs: catfile: %v", err)
		}

		if !info.IsBlob() {
			continue
		}

		newBlobs = append(newBlobs, &gitalypb.NewBlobObject{Oid: info.Oid, Size: info.Size, Path: []byte(parts[1])})
		if len(newBlobs) >= 1000 {
			response := &gitalypb.ListNewBlobsResponse{NewBlobObjects: newBlobs}
			stream.Send(response)
			newBlobs = newBlobs[:0]
		}
	}

	response := &gitalypb.ListNewBlobsResponse{NewBlobObjects: newBlobs}
	stream.Send(response)

	return revList.Wait()
}

func validateCommitID(commitID string) error {
	if match, err := regexp.MatchString(`\A[0-9a-f]{40}\z`, commitID); !match || err != nil {
		return status.Errorf(codes.InvalidArgument, "commit id shoud have 40 hexidecimal characters")
	}

	return nil
}
