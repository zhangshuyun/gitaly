package repository

import (
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

func (s *server) SetInfoAttributes(stream gitalypb.RepositoryService_SetInfoAttributesServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "CreateRepositoryFromBundle: first request failed: %v", err)
	}

	firstRead := false
	reader := streamio.NewReader(func() ([]byte, error) {
		if !firstRead {
			firstRead = true
			return firstRequest.GetAttributes(), nil
		}

		request, err := stream.Recv()
		return request.GetAttributes(), err
	})

	ctx := stream.Context()

	tempDir, err := tempdir.New(ctx, firstRequest.GetRepository())

	if err != nil {
		return helper.ErrInternalf("error when creating temporary directory: %v", err)
	}

	attributesFile, err := ioutil.TempFile(tempDir, "new-attributes")
	if err != nil {
		return helper.ErrInternalf("error when creating temp file: %v", err)
	}

	if _, err = io.Copy(attributesFile, reader); err != nil {
		return helper.ErrInternalf("error copying data to file: %v", err)
	}

	attributesFile.Close()

	repoPath, err := helper.GetRepoPath(firstRequest.GetRepository())
	if err != nil {
		return err
	}

	if err = os.Rename(attributesFile.Name(), filepath.Join(repoPath, "info", "attributes")); err != nil {
		return helper.ErrInternalf("error renaming attributes file: %v", err)
	}

	return stream.SendAndClose(&gitalypb.SetInfoAttributesResponse{})
}
