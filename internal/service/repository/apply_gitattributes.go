package repository

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

func applyGitattributes(c *catfile.Batch, repoPath string, revision []byte) error {
	infoPath := path.Join(repoPath, "info")
	attributesPath := path.Join(infoPath, "attributes")

	_, err := c.Info(string(revision))
	if err != nil {
		if catfile.IsNotFound(err) {
			return status.Errorf(codes.InvalidArgument, "Revision doesn't exist")
		}

		return err
	}

	blobInfo, err := c.Info(fmt.Sprintf("%s:.gitattributes", revision))
	if err != nil && !catfile.IsNotFound(err) {
		return err
	}

	if catfile.IsNotFound(err) || blobInfo.Type != "blob" {
		// Remove info/attributes file if there's no .gitattributes file
		if err := os.Remove(attributesPath); err != nil && !os.IsNotExist(err) {
			return err
		}

		return nil
	}

	// Create  /info folder if it doesn't exist
	if err := os.MkdirAll(infoPath, 0755); err != nil {
		return err
	}

	tempFile, err := ioutil.TempFile(infoPath, "attributes")
	if err != nil {
		return status.Errorf(codes.Internal, "ApplyGitAttributes: creating temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	blobReader, err := c.Blob(blobInfo.Oid)
	if err != nil {
		return err
	}

	// Write attributes to temp file
	if _, err := io.CopyN(tempFile, blobReader, blobInfo.Size); err != nil {
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	// Rename temp file and return the result
	return os.Rename(tempFile.Name(), attributesPath)
}

func (server) ApplyGitattributes(ctx context.Context, in *gitalypb.ApplyGitattributesRequest) (*gitalypb.ApplyGitattributesResponse, error) {
	repo := in.GetRepository()
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}

	if err := git.ValidateRevision(in.GetRevision()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "ApplyGitAttributes: revision: %v", err)
	}

	c, err := catfile.New(ctx, repo)
	if err != nil {
		return nil, err
	}

	if err := applyGitattributes(c, repoPath, in.GetRevision()); err != nil {
		return nil, err
	}

	return &gitalypb.ApplyGitattributesResponse{}, nil
}
