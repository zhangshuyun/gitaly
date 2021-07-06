package commit

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

var (
	defaultFiles = [][]byte{
		[]byte(".gitattributes"), []byte(".gitignore"), []byte(".gitmodules"),
		[]byte("CHANGELOG"), []byte("CONTRIBUTING.md"), []byte("Gemfile.zip"),
		[]byte("LICENSE"), []byte("MAINTENANCE.md"), []byte("PROCESS.md"),
		[]byte("README"), []byte("README.md"), []byte("VERSION"),
		[]byte("bar/branch-test.txt"), []byte("custom-highlighting/test.gitlab-custom"),
		[]byte("encoding/feature-1.txt"), []byte("encoding/feature-2.txt"),
		[]byte("encoding/hotfix-1.txt"), []byte("encoding/hotfix-2.txt"),
		[]byte("encoding/iso8859.txt"), []byte("encoding/russian.rb"),
		[]byte("encoding/test.txt"), []byte("encoding/テスト.txt"), []byte("encoding/テスト.xls"),
		[]byte("files/html/500.html"), []byte("files/images/6049019_460s.jpg"),
		[]byte("files/images/logo-black.png"), []byte("files/images/logo-white.png"),
		[]byte("files/images/wm.svg"), []byte("files/js/application.js"),
		[]byte("files/js/commit.coffee"), []byte("files/lfs/lfs_object.iso"),
		[]byte("files/markdown/ruby-style-guide.md"), []byte("files/ruby/popen.rb"),
		[]byte("files/ruby/regex.rb"), []byte("files/ruby/version_info.rb"),
		[]byte("files/whitespace"), []byte("foo/bar/.gitkeep"),
		[]byte("gitaly/file-with-multiple-chunks"), []byte("gitaly/logo-white.png"),
		[]byte("gitaly/mode-file"), []byte("gitaly/mode-file-with-mods"),
		[]byte("gitaly/no-newline-at-the-end"), []byte("gitaly/renamed-file"),
		[]byte("gitaly/renamed-file-with-mods"), []byte("gitaly/symlink-to-be-regular"),
		[]byte("gitaly/tab\tnewline\n file"), []byte("gitaly/テスト.txt"),
		[]byte("with space/README.md"),
	}
)

func TestListFiles_success(t *testing.T) {
	defaultBranchName = func(context.Context, git.RepositoryExecutor) ([]byte, error) {
		return []byte("test-do-not-touch"), nil
	}
	defer func() {
		defaultBranchName = ref.DefaultBranchName
	}()

	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	tests := []struct {
		desc     string
		revision string
		files    [][]byte
	}{
		{
			desc:     "valid object ID",
			revision: "54fcc214b94e78d7a41a9a8fe6d87a5e59500e51",
			files: [][]byte{
				[]byte(".gitignore"), []byte(".gitmodules"), []byte("CHANGELOG"),
				[]byte("CONTRIBUTING.md"), []byte("Gemfile.zip"), []byte("LICENSE"),
				[]byte("MAINTENANCE.md"), []byte("PROCESS.md"), []byte("README"),
				[]byte("README.md"), []byte("VERSION"), []byte("encoding/feature-1.txt"),
				[]byte("encoding/feature-2.txt"), []byte("encoding/hotfix-1.txt"), []byte("encoding/hotfix-2.txt"),
				[]byte("encoding/iso8859.txt"), []byte("encoding/russian.rb"), []byte("encoding/test.txt"),
				[]byte("encoding/テスト.txt"), []byte("encoding/テスト.xls"), []byte("files/html/500.html"),
				[]byte("files/images/6049019_460s.jpg"), []byte("files/images/logo-black.png"), []byte("files/images/logo-white.png"),
				[]byte("files/images/wm.svg"), []byte("files/js/application.js"), []byte("files/js/commit.js.coffee"),
				[]byte("files/lfs/lfs_object.iso"), []byte("files/markdown/ruby-style-guide.md"), []byte("files/ruby/popen.rb"),
				[]byte("files/ruby/regex.rb"), []byte("files/ruby/version_info.rb"), []byte("files/whitespace"),
				[]byte("foo/bar/.gitkeep"),
			},
		},
		{
			desc:     "valid branch",
			revision: "test-do-not-touch",
			files:    defaultFiles,
		},
		{
			desc:     "valid fully qualified branch",
			revision: "refs/heads/test-do-not-touch",
			files:    defaultFiles,
		},
		{
			desc:     "missing object ID uses default branch",
			revision: "",
			files:    defaultFiles,
		},
		{
			desc:     "invalid object ID",
			revision: "1234123412341234",
			files:    [][]byte{},
		},
		{
			desc:     "invalid branch",
			revision: "non/existing",
			files:    [][]byte{},
		},
		{
			desc:     "nonexisting fully qualified branch",
			revision: "refs/heads/foobar",
			files:    [][]byte{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := gitalypb.ListFilesRequest{
				Repository: repo, Revision: []byte(tc.revision),
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.ListFiles(ctx, &rpcRequest)
			require.NoError(t, err)

			var files [][]byte
			for {
				resp, err := c.Recv()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				files = append(files, resp.GetPaths()...)
			}

			require.ElementsMatch(t, files, tc.files)
		})
	}
}

func TestListFiles_unbornBranch(t *testing.T) {
	t.Parallel()
	cfg, _, _, client := setupCommitServiceWithRepo(t, true)
	repo, _, _ := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])

	tests := []struct {
		desc     string
		revision string
		code     codes.Code
	}{
		{
			desc:     "HEAD",
			revision: "HEAD",
		},
		{
			desc:     "unborn branch",
			revision: "refs/heads/master",
		},
		{
			desc:     "nonexisting branch",
			revision: "i-dont-exist",
		},
		{
			desc:     "nonexisting fully qualified branch",
			revision: "refs/heads/i-dont-exist",
		},
		{
			desc:     "missing revision without default branch",
			revision: "",
			code:     codes.FailedPrecondition,
		},
		{
			desc:     "valid object ID",
			revision: "54fcc214b94e78d7a41a9a8fe6d87a5e59500e51",
		},
		{
			desc:     "invalid object ID",
			revision: "1234123412341234",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := gitalypb.ListFilesRequest{
				Repository: repo, Revision: []byte(tc.revision),
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.ListFiles(ctx, &rpcRequest)
			require.NoError(t, err)

			var files [][]byte
			for {
				var resp *gitalypb.ListFilesResponse
				resp, err = c.Recv()
				if err != nil {
					break
				}

				require.NoError(t, err)
				files = append(files, resp.GetPaths()...)
			}

			if tc.code != codes.OK {
				testhelper.RequireGrpcError(t, err, tc.code)
			} else {
				require.Equal(t, err, io.EOF)
			}
			require.Empty(t, files)
		})
	}
}

func TestListFiles_failure(t *testing.T) {
	t.Parallel()
	_, _, _, client := setupCommitServiceWithRepo(t, true)

	tests := []struct {
		desc string
		repo *gitalypb.Repository
		code codes.Code
	}{
		{
			desc: "nil repo",
			repo: nil,
			code: codes.InvalidArgument,
		},
		{
			desc: "empty repo object",
			repo: &gitalypb.Repository{},
			code: codes.InvalidArgument,
		},
		{
			desc: "non-existing repo",
			repo: &gitalypb.Repository{StorageName: "foo", RelativePath: "bar"},
			code: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rpcRequest := gitalypb.ListFilesRequest{
				Repository: tc.repo, Revision: []byte("master"),
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.ListFiles(ctx, &rpcRequest)
			require.NoError(t, err)

			err = drainListFilesResponse(c)
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func drainListFilesResponse(c gitalypb.CommitService_ListFilesClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func TestListFiles_invalidRevision(t *testing.T) {
	t.Parallel()
	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.ListFiles(ctx, &gitalypb.ListFilesRequest{
		Repository: repo,
		Revision:   []byte("--output=/meow"),
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}
