package conflicts

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

type conflictFile struct {
	header  *gitalypb.ConflictFileHeader
	content []byte
}

func TestSuccessfulListConflictFilesRequest(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	_, repo, _, client := SetupConflictsService(t, false)

	ourCommitOid := "1a35b5a77cf6af7edf6703f88e82f6aff613666f"
	theirCommitOid := "8309e68585b28d61eb85b7e2834849dda6bf1733"

	conflictContent1 := `<<<<<<< encoding/codagé
Content is not important, file name is
=======
Content can be important, but here, file name is of utmost importance
>>>>>>> encoding/codagé
`
	conflictContent2 := `<<<<<<< files/ruby/feature.rb
class Feature
  def foo
    puts 'bar'
  end
=======
# This file was changed in feature branch
# We put different code here to make merge conflict
class Conflict
>>>>>>> files/ruby/feature.rb
end
`

	request := &gitalypb.ListConflictFilesRequest{
		Repository:     repo,
		OurCommitOid:   ourCommitOid,
		TheirCommitOid: theirCommitOid,
	}

	c, err := client.ListConflictFiles(ctx, request)
	require.NoError(t, err)

	expectedFiles := []*conflictFile{
		{
			header: &gitalypb.ConflictFileHeader{
				CommitOid: ourCommitOid,
				OurMode:   int32(0100644),
				OurPath:   []byte("encoding/codagé"),
				TheirPath: []byte("encoding/codagé"),
			},
			content: []byte(conflictContent1),
		},
		{
			header: &gitalypb.ConflictFileHeader{
				CommitOid: ourCommitOid,
				OurMode:   int32(0100644),
				OurPath:   []byte("files/ruby/feature.rb"),
				TheirPath: []byte("files/ruby/feature.rb"),
			},
			content: []byte(conflictContent2),
		},
	}

	receivedFiles := getConflictFiles(t, c)
	require.Len(t, receivedFiles, len(expectedFiles))

	for i := 0; i < len(expectedFiles); i++ {
		testhelper.ProtoEqual(t, receivedFiles[i].header, expectedFiles[i].header)
		require.Equal(t, expectedFiles[i].content, receivedFiles[i].content)
	}
}

func TestSuccessfulListConflictFilesRequestWithAncestor(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	_, repo, _, client := SetupConflictsService(t, true)

	ourCommitOid := "824be604a34828eb682305f0d963056cfac87b2d"
	theirCommitOid := "1450cd639e0bc6721eb02800169e464f212cde06"

	request := &gitalypb.ListConflictFilesRequest{
		Repository:     repo,
		OurCommitOid:   ourCommitOid,
		TheirCommitOid: theirCommitOid,
	}

	c, err := client.ListConflictFiles(ctx, request)
	require.NoError(t, err)

	expectedFiles := []*conflictFile{
		{
			header: &gitalypb.ConflictFileHeader{
				CommitOid:    ourCommitOid,
				OurMode:      int32(0100644),
				OurPath:      []byte("files/ruby/popen.rb"),
				TheirPath:    []byte("files/ruby/popen.rb"),
				AncestorPath: []byte("files/ruby/popen.rb"),
			},
		},
		{
			header: &gitalypb.ConflictFileHeader{
				CommitOid:    ourCommitOid,
				OurMode:      int32(0100644),
				OurPath:      []byte("files/ruby/regex.rb"),
				TheirPath:    []byte("files/ruby/regex.rb"),
				AncestorPath: []byte("files/ruby/regex.rb"),
			},
		},
	}

	receivedFiles := getConflictFiles(t, c)
	require.Len(t, receivedFiles, len(expectedFiles))

	for i := 0; i < len(expectedFiles); i++ {
		testhelper.ProtoEqual(t, receivedFiles[i].header, expectedFiles[i].header)
	}
}

func TestListConflictFilesHugeDiff(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	cfg, repo, repoPath, client := SetupConflictsService(t, false)

	our := buildCommit(t, ctx, cfg, repo, repoPath, map[string][]byte{
		"a": bytes.Repeat([]byte("a\n"), 128*1024),
		"b": bytes.Repeat([]byte("b\n"), 128*1024),
	})

	their := buildCommit(t, ctx, cfg, repo, repoPath, map[string][]byte{
		"a": bytes.Repeat([]byte("x\n"), 128*1024),
		"b": bytes.Repeat([]byte("y\n"), 128*1024),
	})

	request := &gitalypb.ListConflictFilesRequest{
		Repository:     repo,
		OurCommitOid:   our,
		TheirCommitOid: their,
	}

	c, err := client.ListConflictFiles(ctx, request)
	require.NoError(t, err)

	receivedFiles := getConflictFiles(t, c)
	require.Len(t, receivedFiles, 2)
	testhelper.ProtoEqual(t, &gitalypb.ConflictFileHeader{
		CommitOid: our,
		OurMode:   int32(0100644),
		OurPath:   []byte("a"),
		TheirPath: []byte("a"),
	}, receivedFiles[0].header)

	testhelper.ProtoEqual(t, &gitalypb.ConflictFileHeader{
		CommitOid: our,
		OurMode:   int32(0100644),
		OurPath:   []byte("b"),
		TheirPath: []byte("b"),
	}, receivedFiles[1].header)
}

func buildCommit(t *testing.T, ctx context.Context, cfg config.Cfg, repo *gitalypb.Repository, repoPath string, files map[string][]byte) string {
	t.Helper()

	for file, contents := range files {
		filePath := filepath.Join(repoPath, file)
		require.NoError(t, ioutil.WriteFile(filePath, contents, 0666))
		testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "add", filePath)
	}

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "commit", "-m", "message")

	oid, err := localrepo.New(git.NewExecCommandFactory(cfg), repo, cfg).ResolveRevision(ctx, git.Revision("HEAD"))
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "reset", "--hard", "HEAD~")

	return oid.String()
}

func TestListConflictFilesFailedPrecondition(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	_, repo, _, client := SetupConflictsService(t, true)

	testCases := []struct {
		desc           string
		ourCommitOid   string
		theirCommitOid string
	}{
		{
			desc:           "conflict side missing",
			ourCommitOid:   "eb227b3e214624708c474bdab7bde7afc17cefcc",
			theirCommitOid: "824be604a34828eb682305f0d963056cfac87b2d",
		},
		{
			// These commits have a conflict on the 'VERSION' file in the test repo.
			// The conflict is expected to raise an encoding error.
			desc:           "encoding error",
			ourCommitOid:   "bd493d44ae3c4dd84ce89cb75be78c4708cbd548",
			theirCommitOid: "7df99c9ad5b8c9bfc5ae4fb7a91cc87adcce02ef",
		},
		{
			desc:           "submodule object lookup error",
			ourCommitOid:   "de78448b0b504f3f60093727bddfda1ceee42345",
			theirCommitOid: "2f61d70f862c6a4f782ef7933e020a118282db29",
		},
		{
			desc:           "invalid commit id on 'our' side",
			ourCommitOid:   "abcdef0000000000000000000000000000000000",
			theirCommitOid: "1a35b5a77cf6af7edf6703f88e82f6aff613666f",
		},
		{
			desc:           "invalid commit id on 'their' side",
			ourCommitOid:   "1a35b5a77cf6af7edf6703f88e82f6aff613666f",
			theirCommitOid: "abcdef0000000000000000000000000000000000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request := &gitalypb.ListConflictFilesRequest{
				Repository:     repo,
				OurCommitOid:   tc.ourCommitOid,
				TheirCommitOid: tc.theirCommitOid,
			}

			c, err := client.ListConflictFiles(ctx, request)
			if err == nil {
				err = drainListConflictFilesResponse(c)
			}

			testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
		})
	}
}

func TestFailedListConflictFilesRequestDueToValidation(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	_, repo, _, client := SetupConflictsService(t, true)

	ourCommitOid := "0b4bc9a49b562e85de7cc9e834518ea6828729b9"
	theirCommitOid := "bb5206fee213d983da88c47f9cf4cc6caf9c66dc"

	testCases := []struct {
		desc    string
		request *gitalypb.ListConflictFilesRequest
		code    codes.Code
	}{
		{
			desc: "empty repo",
			request: &gitalypb.ListConflictFilesRequest{
				Repository:     nil,
				OurCommitOid:   ourCommitOid,
				TheirCommitOid: theirCommitOid,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty OurCommitId field",
			request: &gitalypb.ListConflictFilesRequest{
				Repository:     repo,
				OurCommitOid:   "",
				TheirCommitOid: theirCommitOid,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty TheirCommitId field",
			request: &gitalypb.ListConflictFilesRequest{
				Repository:     repo,
				OurCommitOid:   ourCommitOid,
				TheirCommitOid: "",
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			c, _ := client.ListConflictFiles(ctx, testCase.request)
			testhelper.RequireGrpcError(t, drainListConflictFilesResponse(c), testCase.code)
		})
	}
}

func getConflictFiles(t *testing.T, c gitalypb.ConflictsService_ListConflictFilesClient) []*conflictFile {
	t.Helper()

	var files []*conflictFile
	var currentFile *conflictFile

	for {
		r, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for _, file := range r.GetFiles() {
			// If there's a header this is the beginning of a new file
			if header := file.GetHeader(); header != nil {
				if currentFile != nil {
					files = append(files, currentFile)
				}

				currentFile = &conflictFile{header: header}
			} else {
				// Append to current file's content
				currentFile.content = append(currentFile.content, file.GetContent()...)
			}
		}
	}

	// Append leftover file
	files = append(files, currentFile)

	return files
}

func drainListConflictFilesResponse(c gitalypb.ConflictsService_ListConflictFilesClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
