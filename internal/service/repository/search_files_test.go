package repository

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

	stop, serverSocketPath := runRepoServer(t)
	defer stop()

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
		{
			desc:   "no results",
			query:  "这个应该没有结果",
			ref:    "many_files",
			output: [][]byte{},
		},
		{
			desc:   "with regexp limiter only recognized by pcre",
			query:  "(*LIMIT_MATCH=1)foobar",
			ref:    "many_files",
			output: contentOutputLines,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request := &gitalypb.SearchFilesByContentRequest{
				Repository: testRepo,
				Query:      tc.query,
				Ref:        []byte(tc.ref),
			}
			request.ChunkedResponse = true
			stream, err := client.SearchFilesByContent(ctx, request)
			require.NoError(t, err)

			resp, err := consumeFilenameByContentChunked(stream)
			require.NoError(t, err)
			require.Equal(t, len(tc.output), len(resp))
			for i := 0; i < len(tc.output); i++ {
				require.Equal(t, tc.output[i], resp[i])
			}
		})
	}
}

func TestSearchFilesByContentLargeFile(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepoWithWorktree(t)
	defer cleanupFn()

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	largeFiles := []struct {
		filename string
		line     string
		repeated int
		query    string
	}{
		{
			filename: "large_file_of_abcdefg_2mb",
			line:     "abcdefghi\n", // 10 bytes
			repeated: 210000,
			query:    "abcdefg",
		},
		{
			filename: "large_file_of_unicode_1.5mb",
			line:     "你见天吃了什么东西?\n", // 22 bytes
			repeated: 70000,
			query:    "什么东西",
		},
	}

	for _, largeFile := range largeFiles {
		t.Run(largeFile.filename, func(t *testing.T) {
			require.NoError(t, ioutil.WriteFile(path.Join(testRepoPath, largeFile.filename), bytes.Repeat([]byte(largeFile.line), largeFile.repeated), 0644))
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "add", ".")
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath,
				"-c", fmt.Sprintf("user.name=%s", committerName),
				"-c", fmt.Sprintf("user.email=%s", committerEmail), "commit", "-m", "large file commit", "--", largeFile.filename)

			stream, err := client.SearchFilesByContent(ctx, &gitalypb.SearchFilesByContentRequest{
				Repository:      testRepo,
				Query:           largeFile.query,
				Ref:             []byte("master"),
				ChunkedResponse: true,
			})
			require.NoError(t, err)

			resp, err := consumeFilenameByContentChunked(stream)
			require.NoError(t, err)

			require.Equal(t, largeFile.repeated, len(bytes.Split(bytes.TrimRight(resp[0], "\n"), []byte("\n"))))
		})
	}
}

func TestSearchFilesByContentFailure(t *testing.T) {
	server := NewServer(RubyServer, config.GitalyInternalSocketPath())

	testRepo, _, cleanupRepo := testhelper.NewTestRepo(t)
	defer cleanupRepo()

	testCases := []struct {
		desc  string
		repo  *gitalypb.Repository
		query string
		ref   string
		code  codes.Code
	}{
		{
			desc: "empty request",
			code: codes.InvalidArgument,
		},
		{
			desc:  "only query given",
			query: "foo",
			code:  codes.InvalidArgument,
		},
		{
			desc:  "no repo",
			query: "foo",
			ref:   "master",
			code:  codes.InvalidArgument,
		},
		{
			desc:  "invalid ref argument",
			repo:  testRepo,
			query: ".",
			ref:   "--no-index",
			code:  codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := server.SearchFilesByContent(&gitalypb.SearchFilesByContentRequest{
				Repository: tc.repo,
				Query:      tc.query,
				Ref:        []byte(tc.ref),
			}, nil)

			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func TestSearchFilesByNameSuccessful(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runRepoServer(t)
	defer stop()

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
		stream, err := client.SearchFilesByName(ctx, &gitalypb.SearchFilesByNameRequest{
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

	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc  string
		repo  *gitalypb.Repository
		query string
		ref   string
		code  codes.Code
	}{
		{
			desc: "empty request",
			code: codes.InvalidArgument,
		},
		{
			desc:  "only query given",
			query: "foo",
			code:  codes.InvalidArgument,
		},
		{
			desc:  "no repo",
			query: "foo",
			ref:   "master",
			code:  codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.SearchFilesByName(ctx, &gitalypb.SearchFilesByNameRequest{
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

func consumeFilenameByContentChunked(stream gitalypb.RepositoryService_SearchFilesByContentClient) ([][]byte, error) {
	var ret [][]byte
	var match []byte

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		match = append(match, resp.MatchData...)
		if resp.EndOfMatch {
			ret = append(ret, match)
			match = nil
		}
	}

	return ret, nil
}

func consumeFilenameByName(stream gitalypb.RepositoryService_SearchFilesByNameClient) ([][]byte, error) {
	var ret [][]byte

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
