package commit

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

func TestListFilesSuccess(t *testing.T) {
	defaultBranchName = func(ctx context.Context, _ *gitalypb.Repository) ([]byte, error) {
		return []byte("test-do-not-touch"), nil
	}
	defer func() {
		defaultBranchName = ref.DefaultBranchName
	}()

	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tests := []struct {
		revision string
		files    [][]byte
	}{
		{ // Valid SHA
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
		{ // valid branch
			revision: "test-do-not-touch",
			files:    defaultFiles,
		},
		{ // no SHA => master
			revision: "",
			files:    defaultFiles,
		},
		{ // Invalid SHA
			revision: "1234123412341234",
			files:    [][]byte{},
		},
		{ // Invalid Branch
			revision: "non/existing",
			files:    [][]byte{},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("test case: %q", test.revision), func(t *testing.T) {
			var files [][]byte
			rpcRequest := gitalypb.ListFilesRequest{
				Repository: testRepo, Revision: []byte(test.revision),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.ListFiles(ctx, &rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

			for {
				resp, err := c.Recv()
				if err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				files = append(files, resp.GetPaths()...)
			}

			if len(files) != len(test.files) {
				t.Errorf("incorrect number of files: %d != %d", len(files), len(test.files))
				return
			}

			for i := range files {
				if !bytes.Equal(files[i], test.files[i]) {
					t.Errorf("%q != %q", files[i], test.files[i])
				}
			}
		})
	}
}

func TestListFilesFailure(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		repo *gitalypb.Repository
		code codes.Code
		desc string
	}{
		// Nil Repo
		{repo: nil, code: codes.InvalidArgument, desc: "nil repo"},
		// Empty Repo Object
		{repo: &gitalypb.Repository{}, code: codes.InvalidArgument, desc: "empty repo object"},
		// Non-existing Repo
		{repo: &gitalypb.Repository{StorageName: "foo", RelativePath: "bar"}, code: codes.InvalidArgument, desc: "non-existing repo"},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			rpcRequest := gitalypb.ListFilesRequest{
				Repository: test.repo, Revision: []byte("master"),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.ListFiles(ctx, &rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

			err = drainListFilesResponse(c)
			testhelper.RequireGrpcError(t, err, test.code)
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

func TestInvalidListFilesRequestRevision(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.ListFiles(ctx, &gitalypb.ListFilesRequest{
		Repository: testRepo,
		Revision:   []byte("--output=/meow"),
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}
