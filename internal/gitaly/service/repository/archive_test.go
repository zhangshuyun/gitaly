package repository

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/codes"
)

const (
	secretToken = "topsecret"
	lfsBody     = "hello world\n"
)

func TestGetArchiveSuccess(t *testing.T) {
	_, repo, _, client := setupRepositoryService(t)

	formats := []gitalypb.GetArchiveRequest_Format{
		gitalypb.GetArchiveRequest_ZIP,
		gitalypb.GetArchiveRequest_TAR,
		gitalypb.GetArchiveRequest_TAR_GZ,
		gitalypb.GetArchiveRequest_TAR_BZ2,
	}

	testCases := []struct {
		desc      string
		prefix    string
		commitID  string
		path      []byte
		exclude   [][]byte
		elidePath bool
		contents  []string
		excluded  []string
	}{
		{
			desc:     "without-prefix",
			commitID: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			prefix:   "",
			contents: []string{"/.gitignore", "/LICENSE", "/README.md"},
		},
		{
			desc:     "with-prefix",
			commitID: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			prefix:   "my-prefix",
			contents: []string{"/.gitignore", "/LICENSE", "/README.md"},
		},
		{
			desc:     "with path as blank string",
			commitID: "1e292f8fedd741b75372e19097c76d327140c312",
			prefix:   "",
			path:     []byte(""),
			contents: []string{"/.gitignore", "/LICENSE", "/README.md"},
		},
		{
			desc:     "with path as nil",
			commitID: "1e292f8fedd741b75372e19097c76d327140c312",
			prefix:   "",
			path:     nil,
			contents: []string{"/.gitignore", "/LICENSE", "/README.md"},
		},
		{
			desc:     "with path",
			commitID: "1e292f8fedd741b75372e19097c76d327140c312",
			prefix:   "",
			path:     []byte("files"),
			contents: []string{"/whitespace", "/html/500.html"},
		},
		{
			desc:     "with path and trailing slash",
			commitID: "1e292f8fedd741b75372e19097c76d327140c312",
			prefix:   "",
			path:     []byte("files/"),
			contents: []string{"/whitespace", "/html/500.html"},
		},
		{
			desc:     "with exclusion",
			commitID: "1e292f8fedd741b75372e19097c76d327140c312",
			prefix:   "",
			exclude:  [][]byte{[]byte("files")},
			contents: []string{"/.gitignore", "/LICENSE", "/README.md"},
			excluded: []string{"/files/whitespace", "/files/html/500.html"},
		},
		{
			desc:      "with path elision",
			commitID:  "1e292f8fedd741b75372e19097c76d327140c312",
			prefix:    "my-prefix",
			elidePath: true,
			path:      []byte("files/"),
			contents:  []string{"/whitespace", "/html/500.html"},
		},
		{
			desc:      "with path elision and exclusion",
			commitID:  "1e292f8fedd741b75372e19097c76d327140c312",
			prefix:    "my-prefix",
			elidePath: true,
			path:      []byte("files/"),
			exclude:   [][]byte{[]byte("files/images")},
			contents:  []string{"/whitespace", "/html/500.html"},
			excluded:  []string{"/images/emoji.png"},
		},
		{
			desc:      "with path elision at root",
			commitID:  "1e292f8fedd741b75372e19097c76d327140c312",
			prefix:    "my-prefix",
			elidePath: true,
			contents:  []string{"/files/whitespace", "/files/html/500.html"},
		},
	}

	for _, tc := range testCases {
		// Run test case with each format
		for _, format := range formats {
			testCaseName := fmt.Sprintf("%s-%s", tc.desc, format.String())
			t.Run(testCaseName, func(t *testing.T) {
				ctx, cancel := testhelper.Context()
				defer cancel()

				req := &gitalypb.GetArchiveRequest{
					Repository: repo,
					CommitId:   tc.commitID,
					Prefix:     tc.prefix,
					Format:     format,
					Path:       tc.path,
					Exclude:    tc.exclude,
					ElidePath:  tc.elidePath,
				}
				stream, err := client.GetArchive(ctx, req)
				require.NoError(t, err)

				data, err := consumeArchive(stream)
				require.NoError(t, err)

				archiveFile, err := ioutil.TempFile("", "")
				require.NoError(t, err)
				defer os.Remove(archiveFile.Name())

				_, err = archiveFile.Write(data)
				require.NoError(t, err)

				contents := string(compressedFileContents(t, format, archiveFile.Name()))

				for _, content := range tc.contents {
					require.Contains(t, contents, tc.prefix+content)
				}

				for _, excluded := range tc.excluded {
					require.NotContains(t, contents, tc.prefix+excluded)
				}
			})
		}
	}
}

func TestGetArchiveWithLfsSuccess(t *testing.T) {
	defaultOptions := testhelper.GitlabTestServerOptions{
		SecretToken: secretToken,
		LfsBody:     lfsBody,
	}

	url, cleanup := testhelper.NewGitlabTestServer(t, defaultOptions)
	t.Cleanup(cleanup)

	shellDir := testhelper.TempDir(t)

	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
		GitlabShell: config.GitlabShell{Dir: shellDir},
		Gitlab: config.Gitlab{
			URL:        url,
			SecretFile: testhelper.WriteShellSecretFile(t, shellDir, defaultOptions.SecretToken),
		},
	}))

	serverSocketPath := runRepositoryServerWithConfig(t, cfg, nil)
	client := newRepositoryClient(t, cfg, serverSocketPath)

	repo, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)

	testhelper.ConfigureGitalyLfsSmudge(t, cfg.BinDir)

	// lfs-moar branch SHA
	sha := "46abbb087fcc0fd02c340f0f2f052bd2c7708da3"

	testCases := []struct {
		desc            string
		prefix          string
		path            []byte
		includeLfsBlobs bool
	}{
		{
			desc:            "without prefix and with LFS blobs",
			prefix:          "",
			includeLfsBlobs: true,
		},
		{
			desc:            "without prefix and without LFS blobs",
			prefix:          "",
			includeLfsBlobs: false,
		},
		{
			desc:            "with prefix and with LFS blobs",
			prefix:          "my-prefix",
			includeLfsBlobs: true,
		},
		{
			desc:            "with prefix and without LFS blobs",
			prefix:          "my-prefix",
			includeLfsBlobs: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			req := &gitalypb.GetArchiveRequest{
				Repository:      repo,
				CommitId:        sha,
				Prefix:          tc.prefix,
				Format:          gitalypb.GetArchiveRequest_ZIP,
				Path:            tc.path,
				IncludeLfsBlobs: tc.includeLfsBlobs,
			}
			stream, err := client.GetArchive(ctx, req)
			require.NoError(t, err)

			data, err := consumeArchive(stream)
			require.NoError(t, err)
			reader := bytes.NewReader(data)

			zipReader, err := zip.NewReader(reader, int64(reader.Len()))
			require.NoError(t, err)

			lfsFiles := []string{"/30170.lfs", "/another.lfs"}
			for _, lfsFile := range lfsFiles {
				found := false
				for _, f := range zipReader.File {
					if f.Name != tc.prefix+lfsFile {
						continue
					}

					found = true

					fc, err := f.Open()
					require.NoError(t, err)
					defer fc.Close()

					data, err := ioutil.ReadAll(fc)
					require.NoError(t, err)

					if tc.includeLfsBlobs {
						require.Equal(t, lfsBody, string(data))
					} else {
						require.Contains(t, string(data), "oid sha256:")
					}
				}

				require.True(t, found, "expected to find LFS file")
			}
		})
	}
}

func TestGetArchiveFailure(t *testing.T) {
	_, repo, _, client := setupRepositoryService(t)

	commitID := "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"

	testCases := []struct {
		desc      string
		repo      *gitalypb.Repository
		prefix    string
		commitID  string
		format    gitalypb.GetArchiveRequest_Format
		path      []byte
		exclude   [][]byte
		elidePath bool
		code      codes.Code
	}{
		{
			desc:     "Repository doesn't exist",
			repo:     &gitalypb.Repository{StorageName: "fake", RelativePath: "path"},
			prefix:   "",
			commitID: commitID,
			format:   gitalypb.GetArchiveRequest_ZIP,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "Repository is nil",
			repo:     nil,
			prefix:   "",
			commitID: commitID,
			format:   gitalypb.GetArchiveRequest_ZIP,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "CommitId is empty",
			repo:     repo,
			prefix:   "",
			commitID: "",
			format:   gitalypb.GetArchiveRequest_ZIP,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "Format is invalid",
			repo:     repo,
			prefix:   "",
			commitID: "",
			format:   gitalypb.GetArchiveRequest_Format(-1),
			code:     codes.InvalidArgument,
		},
		{
			desc:     "Non-existing path in repository",
			repo:     repo,
			prefix:   "",
			commitID: "1e292f8fedd741b75372e19097c76d327140c312",
			format:   gitalypb.GetArchiveRequest_ZIP,
			path:     []byte("unknown-path"),
			code:     codes.FailedPrecondition,
		},
		{
			desc:     "Non-existing path in repository on commit ID",
			repo:     repo,
			prefix:   "",
			commitID: commitID,
			format:   gitalypb.GetArchiveRequest_ZIP,
			path:     []byte("files/"),
			code:     codes.FailedPrecondition,
		},
		{
			desc:     "Non-existing exclude path in repository on commit ID",
			repo:     repo,
			prefix:   "",
			commitID: commitID,
			format:   gitalypb.GetArchiveRequest_ZIP,
			exclude:  [][]byte{[]byte("files/")},
			code:     codes.FailedPrecondition,
		},
		{
			desc:     "path contains directory traversal outside repository root",
			repo:     repo,
			prefix:   "",
			commitID: "1e292f8fedd741b75372e19097c76d327140c312",
			format:   gitalypb.GetArchiveRequest_ZIP,
			path:     []byte("../../foo"),
			code:     codes.InvalidArgument,
		},
		{
			desc:     "repo missing fields",
			repo:     &gitalypb.Repository{StorageName: repo.GetStorageName()},
			prefix:   "qwert",
			commitID: "sadf",
			format:   gitalypb.GetArchiveRequest_TAR,
			path:     []byte("Here is a string...."),
			code:     codes.InvalidArgument,
		},
		{
			desc:      "with path is file and path elision",
			repo:      repo,
			commitID:  "1e292f8fedd741b75372e19097c76d327140c312",
			prefix:    "my-prefix",
			elidePath: true,
			path:      []byte("files/html/500.html"),
			code:      codes.Unknown,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			req := &gitalypb.GetArchiveRequest{
				Repository: tc.repo,
				CommitId:   tc.commitID,
				Prefix:     tc.prefix,
				Format:     tc.format,
				Path:       tc.path,
				Exclude:    tc.exclude,
				ElidePath:  tc.elidePath,
			}
			stream, err := client.GetArchive(ctx, req)
			require.NoError(t, err)

			_, err = consumeArchive(stream)
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func TestGetArchivePathInjection(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryServiceWithWorktree(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	// Adding a temp directory representing the .ssh directory
	sshDirectory := testhelper.TempDir(t)

	// Adding an empty authorized_keys file
	authorizedKeysPath := filepath.Join(sshDirectory, "authorized_keys")

	authorizedKeysFile, err := os.Create(authorizedKeysPath)
	require.NoError(t, err)
	require.NoError(t, authorizedKeysFile.Close())

	// Create the directory on the repository
	repoExploitPath := filepath.Join(repoPath, "--output=", authorizedKeysPath)
	require.NoError(t, os.MkdirAll(repoExploitPath, os.ModeDir|0755))

	f, err := os.Create(filepath.Join(repoExploitPath, "id_12345.pub"))
	require.NoError(t, err)

	evilPubKeyFile := `#
		ssh-ed25519 my_super_evil_ssh_pubkey
		#`

	_, err = fmt.Fprint(f, evilPubKeyFile)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Add the directory to the repository
	gittest.Exec(t, cfg, "-C", repoPath, "add", ".")
	gittest.Exec(t, cfg, "-C", repoPath, "commit", "-m", "adding fake key file")
	commitID := strings.TrimRight(string(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "HEAD")), "\n")

	injectionPath := fmt.Sprintf("--output=%s", authorizedKeysPath)

	req := &gitalypb.GetArchiveRequest{
		Repository: repo,
		CommitId:   commitID,
		Prefix:     "",
		Format:     gitalypb.GetArchiveRequest_TAR,
		Path:       []byte(injectionPath),
	}

	stream, err := client.GetArchive(ctx, req)
	require.NoError(t, err)

	_, err = consumeArchive(stream)
	require.NoError(t, err)

	authorizedKeysFile, err = os.Open(authorizedKeysPath)
	require.NoError(t, err)
	defer authorizedKeysFile.Close()

	authorizedKeysFileBytes, err := ioutil.ReadAll(authorizedKeysFile)
	require.NoError(t, err)
	authorizedKeysFileStat, err := authorizedKeysFile.Stat()
	require.NoError(t, err)

	require.NotContains(t, string(authorizedKeysFileBytes), evilPubKeyFile) // this should fail first in pre-fix failing test
	require.Zero(t, authorizedKeysFileStat.Size())
}

func TestGetArchiveEnv(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", "archive.sh")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	err = tmpFile.Chmod(0755)
	require.NoError(t, err)

	_, err = tmpFile.Write([]byte(`#!/bin/sh
env | grep -E "^GL_|CORRELATION|GITALY_"`))
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	cfg := testcfg.Build(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	// We re-define path to the git executable to catch parameters used to call it.
	// This replacement only needs to be done for the configuration used to invoke git commands.
	// Other operations should use actual path to the git binary to work properly.
	spyGitCfg := cfg
	spyGitCfg.Git.BinPath = tmpFile.Name()

	serverSocketPath := runRepositoryServerWithConfig(t, spyGitCfg, nil)
	cfg.SocketPath = serverSocketPath

	client := newRepositoryClient(t, cfg, serverSocketPath)

	repo, _, cleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg, cfg.Storages[0])
	t.Cleanup(cleanup)

	commitID := "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"

	ctx, cancel := testhelper.Context()
	defer cancel()

	correlationID, _ := correlation.RandomID()
	ctx = correlation.ContextWithCorrelation(ctx, correlationID)

	req := &gitalypb.GetArchiveRequest{
		Repository: repo,
		CommitId:   commitID,
	}

	cfgData, err := json.Marshal(cfg.Gitlab)
	require.NoError(t, err)

	tlsCfgData, err := json.Marshal(cfg.TLS)
	require.NoError(t, err)

	stream, err := client.GetArchive(ctx, req)
	require.NoError(t, err)

	data, err := consumeArchive(stream)
	require.NoError(t, err)
	require.Contains(t, string(data), "GL_REPOSITORY="+gittest.GlRepository)
	require.Contains(t, string(data), "GL_PROJECT_PATH="+gittest.GlProjectPath)
	require.Contains(t, string(data), "GL_INTERNAL_CONFIG="+string(cfgData))
	require.Contains(t, string(data), "GITALY_TLS="+string(tlsCfgData))
	require.Contains(t, string(data), "CORRELATION_ID="+correlationID)
	require.Contains(t, string(data), "GITALY_LOG_DIR="+cfg.Logging.Dir)
}

func compressedFileContents(t *testing.T, format gitalypb.GetArchiveRequest_Format, name string) []byte {
	switch format {
	case gitalypb.GetArchiveRequest_TAR:
		return testhelper.MustRunCommand(t, nil, "tar", "tf", name)
	case gitalypb.GetArchiveRequest_TAR_GZ:
		return testhelper.MustRunCommand(t, nil, "tar", "ztf", name)
	case gitalypb.GetArchiveRequest_TAR_BZ2:
		return testhelper.MustRunCommand(t, nil, "tar", "jtf", name)
	case gitalypb.GetArchiveRequest_ZIP:
		return testhelper.MustRunCommand(t, nil, "unzip", "-l", name)
	}

	return nil
}

func consumeArchive(stream gitalypb.RepositoryService_GetArchiveClient) ([]byte, error) {
	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetData(), err
	})

	return ioutil.ReadAll(reader)
}
