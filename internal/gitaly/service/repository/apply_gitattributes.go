package repository

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const attributesFileMode os.FileMode = 0644

func applyGitattributes(c *catfile.Batch, repoPath string, revision []byte) error {
	infoPath := filepath.Join(repoPath, "info")
	attributesPath := filepath.Join(infoPath, "attributes")

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

	blobObj, err := c.Blob(blobInfo.Oid)
	if err != nil {
		return err
	}

	// Write attributes to temp file
	if _, err := io.CopyN(tempFile, blobObj.Reader, blobInfo.Size); err != nil {
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	// Change the permission of tempFile as the permission of file attributesPath
	if err := os.Chmod(tempFile.Name(), attributesFileMode); err != nil {
		return err
	}

	// Rename temp file and return the result
	return os.Rename(tempFile.Name(), attributesPath)
}

func (s *server) ApplyGitattributes(ctx context.Context, in *gitalypb.ApplyGitattributesRequest) (*gitalypb.ApplyGitattributesResponse, error) {
	repo := in.GetRepository()
	repoPath, err := s.locator.GetRepoPath(repo)
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
