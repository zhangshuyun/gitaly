package repository

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetConfig(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	getConfig := func(
		t *testing.T,
		client gitalypb.RepositoryServiceClient,
		repo *gitalypb.Repository,
	) (string, error) {
		ctx, cleanup := testhelper.Context()
		defer cleanup()

		stream, err := client.GetConfig(ctx, &gitalypb.GetConfigRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		reader := streamio.NewReader(func() ([]byte, error) {
			response, err := stream.Recv()
			var bytes []byte
			if response != nil {
				bytes = response.Data
			}
			return bytes, err
		})

		contents, err := io.ReadAll(reader)
		return string(contents), err
	}

	t.Run("normal repo", func(t *testing.T) {
		repo, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])

		config, err := getConfig(t, client, repo)
		require.NoError(t, err)

		expectedConfig := "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = true\n"

		if runtime.GOOS == "darwin" {
			expectedConfig = expectedConfig + "\tignorecase = true\n\tprecomposeunicode = true\n"
		}
		require.Equal(t, expectedConfig, config)
	})

	t.Run("missing config", func(t *testing.T) {
		repo, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

		configPath := filepath.Join(repoPath, "config")
		require.NoError(t, os.Remove(configPath))

		config, err := getConfig(t, client, repo)
		testhelper.GrpcEqualErr(t, status.Errorf(codes.NotFound, "opening gitconfig: open %s: no such file or directory", configPath), err)
		require.Equal(t, "", config)
	})
}
