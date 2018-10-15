package repository

import (
	"io"
	"os"
	"path"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) GetInfoAttributes(in *gitalypb.GetInfoAttributesRequest, stream gitalypb.RepositoryService_GetInfoAttributesServer) error {
	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return err
	}

	attrFile := path.Join(repoPath, "info", "attributes")
	f, err := os.Open(attrFile)
	if err != nil {
		if os.IsNotExist(err) {
			stream.Send(&gitalypb.GetInfoAttributesResponse{})
			return nil
		}

		return status.Errorf(codes.Internal, "GetInfoAttributes failure to read info attributes: %v", err)
	}

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetInfoAttributesResponse{
			Attributes: p,
		})
	})

	_, err = io.Copy(sw, f)
	return err
}
