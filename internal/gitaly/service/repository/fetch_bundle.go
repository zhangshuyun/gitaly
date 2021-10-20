package repository

import (
	"io"
	"os"
	"path/filepath"

	gitalyerrors "gitlab.com/gitlab-org/gitaly/v14/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
)

const (
	mirrorRefSpec = "+refs/*:refs/*"
)

func (s *server) FetchBundle(stream gitalypb.RepositoryService_FetchBundleServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternalf("first request: %v", err)
	}

	if firstRequest.GetRepository() == nil {
		return helper.ErrInvalidArgument(gitalyerrors.ErrEmptyRepository)
	}

	firstRead := true
	reader := streamio.NewReader(func() ([]byte, error) {
		if firstRead {
			firstRead = false
			return firstRequest.GetData(), nil
		}

		request, err := stream.Recv()
		return request.GetData(), err
	})

	ctx := stream.Context()
	repo := s.localrepo(firstRequest.GetRepository())

	tmpDir, err := tempdir.New(ctx, repo.GetStorageName(), s.locator)
	if err != nil {
		return helper.ErrInternal(err)
	}

	bundlePath := filepath.Join(tmpDir.Path(), "repo.bundle")
	file, err := os.Create(bundlePath)
	if err != nil {
		return helper.ErrInternal(err)
	}

	_, err = io.Copy(file, reader)
	if err != nil {
		return helper.ErrInternalf("copy bundle: %w", err)
	}

	config := []git.ConfigPair{
		{Key: "remote.inmemory.url", Value: bundlePath},
		{Key: "remote.inmemory.fetch", Value: mirrorRefSpec},
	}
	opts := localrepo.FetchOpts{
		CommandOptions: []git.CmdOpt{git.WithConfigEnv(config...)},
	}

	if err := repo.FetchRemote(ctx, "inmemory", opts); err != nil {
		return helper.ErrInternal(err)
	}

	return stream.SendAndClose(&gitalypb.FetchBundleResponse{})
}
