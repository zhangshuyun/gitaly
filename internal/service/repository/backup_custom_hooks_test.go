package repository

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func TestSuccessfullBackupCustomHooksRequest(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repoPath, err := helper.GetPath(testRepo)
	require.NoError(t, err)

	expectedTarResponse := []string{
		"custom_hooks/",
		"custom_hooks/pre-commit.sample",
		"custom_hooks/prepare-commit-msg.sample",
		"custom_hooks/pre-push.sample",
	}
	require.NoError(t, os.Mkdir(path.Join(repoPath, "custom_hooks"), 0700), "Could not create custom_hooks dir")
	for _, fileName := range expectedTarResponse[1:] {
		require.NoError(t, ioutil.WriteFile(path.Join(repoPath, fileName), []byte("Some hooks"), 0700), fmt.Sprintf("Could not create %s", fileName))
	}

	backupRequest := &gitalypb.BackupCustomHooksRequest{Repository: testRepo}
	backupStream, err := client.BackupCustomHooks(ctx, backupRequest)
	require.NoError(t, err)

	reader := tar.NewReader(streamio.NewReader(func() ([]byte, error) {
		response, err := backupStream.Recv()
		return response.GetData(), err
	}))

	fileLength := 0
	for {
		file, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		fileLength++
		require.Contains(t, expectedTarResponse, file.Name)
	}
	require.Equal(t, fileLength, len(expectedTarResponse))
}

func TestSuccessfullBackupCustomHooksSymlink(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repoPath, err := helper.GetPath(testRepo)
	require.NoError(t, err)

	linkTarget := "/var/empty"
	require.NoError(t, os.Symlink(linkTarget, path.Join(repoPath, "custom_hooks")), "Could not create custom_hooks symlink")

	backupRequest := &gitalypb.BackupCustomHooksRequest{Repository: testRepo}
	backupStream, err := client.BackupCustomHooks(ctx, backupRequest)
	require.NoError(t, err)

	reader := tar.NewReader(streamio.NewReader(func() ([]byte, error) {
		response, err := backupStream.Recv()
		return response.GetData(), err
	}))

	file, err := reader.Next()
	require.NoError(t, err)

	require.Equal(t, "custom_hooks", file.Name, "tar entry name")
	require.Equal(t, byte(tar.TypeSymlink), file.Typeflag, "tar entry type")
	require.Equal(t, linkTarget, file.Linkname, "link target")

	_, err = reader.Next()
	require.Equal(t, io.EOF, err, "custom_hooks should have been the only entry")
}

func TestSuccessfullBackupCustomHooksRequestWithNoHooks(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	backupRequest := &gitalypb.BackupCustomHooksRequest{Repository: testRepo}
	backupStream, err := client.BackupCustomHooks(ctx, backupRequest)
	require.NoError(t, err)

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := backupStream.Recv()
		return response.GetData(), err
	})

	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, reader)
	require.NoError(t, err)

	require.Empty(t, buf, "Returned stream should be empty")
}
