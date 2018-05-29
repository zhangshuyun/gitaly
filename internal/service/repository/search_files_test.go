package repository

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

var (
	contentOutputLines = [][]byte{bytes.Join([][]byte{
		[]byte("many_files:files/markdown/ruby-style-guide.md\x00128\x00    ```Ruby"),
		[]byte("many_files:files/markdown/ruby-style-guide.md\x00129\x00    # bad"),
		[]byte("many_files:files/markdown/ruby-style-guide.md\x00130\x00    puts 'foobar'; # superfluous semicolon"),
		[]byte("many_files:files/markdown/ruby-style-guide.md\x00131\x00"),
		[]byte("many_files:files/markdown/ruby-style-guide.md\x00132\x00    puts 'foo'; puts 'bar' # two expression on the same line"),
		[]byte("many_files:files/markdown/ruby-style-guide.md\x00133\x00"),
		[]byte("many_files:files/markdown/ruby-style-guide.md\x00134\x00    # good"),
		[]byte("many_files:files/markdown/ruby-style-guide.md\x00135\x00    puts 'foobar'"),
		[]byte("many_files:files/markdown/ruby-style-guide.md\x00136\x00"),
		[]byte("many_files:files/markdown/ruby-style-guide.md\x00137\x00    puts 'foo'"),
		[]byte(""),
	}, []byte{'\n'})}
	contentMultiLines = [][]byte{
		bytes.Join([][]byte{
			[]byte("many_files:CHANGELOG\x00306\x00  - Gitlab::Git set of objects to abstract from grit library"),
			[]byte("many_files:CHANGELOG\x00307\x00  - Replace Unicorn web server with Puma"),
			[]byte("many_files:CHANGELOG\x00308\x00  - Backup/Restore refactored. Backup dump project wiki too now"),
			[]byte("many_files:CHANGELOG\x00309\x00  - Restyled Issues list. Show milestone version in issue row"),
			[]byte("many_files:CHANGELOG\x00310\x00  - Restyled Merge Request list"),
			[]byte("many_files:CHANGELOG\x00311\x00  - Backup now dump/restore uploads"),
			[]byte("many_files:CHANGELOG\x00312\x00  - Improved performance of dashboard (Andrew Kumanyaev)"),
			[]byte("many_files:CHANGELOG\x00313\x00  - File history now tracks renames (Akzhan Abdulin)"),
			[]byte(""),
		}, []byte{'\n'}),
		bytes.Join([][]byte{
			[]byte("many_files:CHANGELOG\x00377\x00  - fix routing issues"),
			[]byte("many_files:CHANGELOG\x00378\x00  - cleanup rake tasks"),
			[]byte("many_files:CHANGELOG\x00379\x00  - fix backup/restore"),
			[]byte("many_files:CHANGELOG\x00380\x00  - scss cleanup"),
			[]byte("many_files:CHANGELOG\x00381\x00  - show preview for note images"),
			[]byte(""),
		}, []byte{'\n'}),
		bytes.Join([][]byte{
			[]byte("many_files:CHANGELOG\x00393\x00  - Remove project code and path from API. Use id instead"),
			[]byte("many_files:CHANGELOG\x00394\x00  - Return valid cloneable url to repo for web hook"),
			[]byte("many_files:CHANGELOG\x00395\x00  - Fixed backup issue"),
			[]byte("many_files:CHANGELOG\x00396\x00  - Reorganized settings"),
			[]byte("many_files:CHANGELOG\x00397\x00  - Fixed commits compare"),
			[]byte(""),
		}, []byte{'\n'}),
	}
	contentCoffeeLines = [][]byte{
		bytes.Join([][]byte{
			[]byte("many_files:CONTRIBUTING.md\x0092\x001. [Ruby style guide](https://github.com/bbatsov/ruby-style-guide)"),
			[]byte("many_files:CONTRIBUTING.md\x0093\x001. [Rails style guide](https://github.com/bbatsov/rails-style-guide)"),
			[]byte("many_files:CONTRIBUTING.md\x0094\x001. [CoffeeScript style guide](https://github.com/polarmobile/coffeescript-style-guide)"),
			[]byte("many_files:CONTRIBUTING.md\x0095\x001. [Shell command guidelines](doc/development/shell_commands.md)"),
			[]byte(""),
		}, []byte{'\n'}),
		bytes.Join([][]byte{
			[]byte("many_files:files/js/application.js\x001\x00// This is a manifest file that'll be compiled into including all the files listed below."),
			[]byte("many_files:files/js/application.js\x002\x00// Add new JavaScript/Coffee code in separate files in this directory and they'll automatically"),
			[]byte("many_files:files/js/application.js\x003\x00// be included in the compiled file accessible from http://example.com/assets/application.js"),
			[]byte("many_files:files/js/application.js\x004\x00// It's not advisable to add code directly here, but if you do, it'll appear at the bottom of the"),
			[]byte(""),
		}, []byte{'\n'}),
	}
)

func TestSearchFilesByContentSuccessful(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc   string
		query  string
		ref    string
		output [][]byte
	}{
		{
			desc:   "single file in many_files",
			query:  "foobar",
			ref:    "many_files",
			output: contentOutputLines,
		},
		{
			desc:   "single files, multiple matches",
			query:  "backup",
			ref:    "many_files",
			output: contentMultiLines,
		},
		{
			desc:   "multiple files, multiple matches",
			query:  "coffee",
			ref:    "many_files",
			output: contentCoffeeLines,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.SearchFilesByContent(ctx, &pb.SearchFilesByContentRequest{
				Repository: testRepo,
				Query:      tc.query,
				Ref:        []byte(tc.ref),
			})
			require.NoError(t, err)

			resp, err := consumeFilenameByContent(stream)
			require.NoError(t, err)

			require.NotEmpty(t, resp)
			require.Equal(t, len(tc.output), len(resp))
			for i := 0; i < len(tc.output); i++ {
				require.Equal(t, tc.output[i], resp[i])
			}
		})
	}
}

func TestSearchFilesByContentFailure(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc  string
		repo  *pb.Repository
		query string
		ref   string
		code  codes.Code
		msg   string
	}{
		{
			desc: "empty request",
			code: codes.InvalidArgument,
			msg:  "no query given",
		},
		{
			desc:  "only query given",
			query: "foo",
			code:  codes.InvalidArgument,
			msg:   "no ref given",
		},
		{
			desc:  "no repo",
			query: "foo",
			ref:   "master",
			code:  codes.InvalidArgument,
			msg:   "empty Repo",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			stream, err := client.SearchFilesByContent(ctx, &pb.SearchFilesByContentRequest{
				Repository: tc.repo,
				Query:      tc.query,
				Ref:        []byte(tc.ref),
			})
			require.NoError(t, err)

			_, err = consumeFilenameByContent(stream)
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func TestSearchFilesByNameSuccessful(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		ref      []byte
		query    string
		numFiles int
		testFile []byte
	}{
		{
			ref:      []byte("many_files"),
			query:    "files/images/logo-black.png",
			numFiles: 1,
			testFile: []byte("files/images/logo-black.png"),
		},
		{
			ref:      []byte("many_files"),
			query:    "many_files",
			numFiles: 1001,
			testFile: []byte("many_files/99"),
		},
	}

	for _, tc := range testCases {
		stream, err := client.SearchFilesByName(ctx, &pb.SearchFilesByNameRequest{
			Repository: testRepo,
			Ref:        tc.ref,
			Query:      tc.query,
		})
		require.NoError(t, err)

		var files [][]byte
		files, err = consumeFilenameByName(stream)
		require.NoError(t, err)

		require.Equal(t, tc.numFiles, len(files))
		require.Contains(t, files, tc.testFile)
	}
}

func TestSearchFilesByNameFailure(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc  string
		repo  *pb.Repository
		query string
		ref   string
		code  codes.Code
		msg   string
	}{
		{
			desc: "empty request",
			code: codes.InvalidArgument,
			msg:  "no query given",
		},
		{
			desc:  "only query given",
			query: "foo",
			code:  codes.InvalidArgument,
			msg:   "no ref given",
		},
		{
			desc:  "no repo",
			query: "foo",
			ref:   "master",
			code:  codes.InvalidArgument,
			msg:   "empty Repo",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			stream, err := client.SearchFilesByName(ctx, &pb.SearchFilesByNameRequest{
				Repository: tc.repo,
				Query:      tc.query,
				Ref:        []byte(tc.ref),
			})
			require.NoError(t, err)

			_, err = consumeFilenameByName(stream)
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func consumeFilenameByContent(stream pb.RepositoryService_SearchFilesByContentClient) ([][]byte, error) {
	ret := make([][]byte, 0)
	for done := false; !done; {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ret = append(ret, resp.Matches...)
	}
	return ret, nil
}

func consumeFilenameByName(stream pb.RepositoryService_SearchFilesByNameClient) ([][]byte, error) {
	ret := make([][]byte, 0)
	for done := false; !done; {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ret = append(ret, resp.Files...)
	}
	return ret, nil
}
