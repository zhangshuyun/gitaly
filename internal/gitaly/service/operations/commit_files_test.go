package operations

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	commitFilesMessage = []byte("Change files")
)

func testImplementations(t *testing.T, test func(t *testing.T, ctx context.Context)) {
	goCtx, cancel := testhelper.Context()
	defer cancel()

	rubyCtx := featureflag.OutgoingCtxWithDisabledFeatureFlags(goCtx, featureflag.GoUserCommitFiles)

	for _, tc := range []struct {
		desc    string
		context context.Context
	}{
		{desc: "go", context: goCtx},
		{desc: "ruby", context: rubyCtx},
	} {
		t.Run(tc.desc, func(t *testing.T) { test(t, tc.context) })
	}
}

func TestUserCommitFiles(t *testing.T) {
	testImplementations(t, testUserCommitFiles)
}

func testUserCommitFiles(t *testing.T, ctx context.Context) {
	const (
		DefaultMode    = "100644"
		ExecutableMode = "100755"
	)

	// Multiple locations in the call path depend on the global configuration.
	// This creates a clean directory in the test storage. We then recreate the
	// repository there on every test run. This allows us to use deterministic
	// paths in the tests.
	storageRoot, err := ioutil.TempDir(testhelper.GitlabTestStoragePath(), "")
	require.NoError(t, err)
	defer os.RemoveAll(storageRoot)

	const storageName = "default"
	targetRelativePath, err := filepath.Rel(testhelper.GitlabTestStoragePath(), filepath.Join(storageRoot, "target-repository"))
	require.NoError(t, err)

	startRepo, _, cleanStartRepo := testhelper.InitBareRepo(t)
	defer cleanStartRepo()

	repoPath := filepath.Join(testhelper.GitlabTestStoragePath(), targetRelativePath)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	ctxWithServerMetadata := ctx
	for key, values := range testhelper.GitalyServersMetadata(t, serverSocketPath) {
		for _, value := range values {
			ctxWithServerMetadata = metadata.AppendToOutgoingContext(ctxWithServerMetadata, key, value)
		}
	}

	type step struct {
		actions         []*gitalypb.UserCommitFilesRequest
		startRepository *gitalypb.Repository
		startBranch     string
		error           error
		indexError      string
		repoCreated     bool
		branchCreated   bool
		treeEntries     []testhelper.TreeEntry
	}

	for _, tc := range []struct {
		desc  string
		steps []step
	}{
		{
			desc: "create directory",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "directory-1/.gitkeep"},
					},
				},
			},
		},
		{
			desc: "create directory ignores mode and content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action:          gitalypb.UserCommitFilesActionHeader_CREATE_DIR,
									FilePath:        []byte("directory-1"),
									ExecuteFilemode: true,
									Base64Content:   true,
								},
							},
						}),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "directory-1/.gitkeep"},
					},
				},
			},
		},
		{
			desc: "create directory created duplicate",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
						createDirHeaderRequest("directory-1"),
					},
					indexError: "A directory with this name already exists",
				},
			},
		},
		{
			desc: "create directory with traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("../directory-1"),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "create directory existing duplicate",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "directory-1/.gitkeep"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory-1"),
					},
					indexError: "A directory with this name already exists",
				},
			},
		},
		{
			desc: "create directory with files name",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("file-1"),
					},
					indexError: "A file with this name already exists",
				},
			},
		},
		{
			desc: "create file with directory traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("../file-1"),
						actionContentRequest("content-1"),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "create file with double slash",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("invalid://file/name/here"),
						actionContentRequest("content-1"),
					},
					indexError: "invalid path: 'invalid://file/name/here'",
				},
			},
		},
		{
			desc: "create file without content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1"},
					},
				},
			},
		},
		{
			desc: "create file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						actionContentRequest(" content-2"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1 content-2"},
					},
				},
			},
		},
		{
			desc: "create file with unclean path",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("/file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "create file with base64 content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createBase64FileHeaderRequest("file-1"),
						actionContentRequest(base64.StdEncoding.EncodeToString([]byte("content-1"))),
						actionContentRequest(base64.StdEncoding.EncodeToString([]byte(" content-2"))),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1 content-2"},
					},
				},
			},
		},
		{
			desc: "create file normalizes line endings",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1\r\n"),
						actionContentRequest(" content-2\r\n"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1\n content-2\n"},
					},
				},
			},
		},
		{
			desc: "create duplicate file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						createFileHeaderRequest("file-1"),
					},
					indexError: "A file with this name already exists",
				},
			},
		},
		{
			desc: "create file overwrites directory",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("file-1"),
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "update created file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						updateFileHeaderRequest("file-1"),
						actionContentRequest("content-2"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update file normalizes line endings",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						updateFileHeaderRequest("file-1"),
						actionContentRequest("content-2\r\n"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2\n"},
					},
				},
			},
		},
		{
			desc: "update base64 content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						updateBase64FileHeaderRequest("file-1"),
						actionContentRequest(base64.StdEncoding.EncodeToString([]byte("content-2"))),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update ignores mode",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action:          gitalypb.UserCommitFilesActionHeader_UPDATE,
									FilePath:        []byte("file-1"),
									ExecuteFilemode: true,
								},
							},
						}),
						actionContentRequest("content-2"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						updateFileHeaderRequest("file-1"),
						actionContentRequest("content-2"),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-2"},
					},
				},
			},
		},
		{
			desc: "update non-existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						updateFileHeaderRequest("non-existing"),
						actionContentRequest("content"),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "move file with traversal in source",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("../original-file", "moved-file", true),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "move file with traversal in destination",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("original-file", "../moved-file", true),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "move created file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("content-1"),
						moveFileHeaderRequest("original-file", "moved-file", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "move ignores mode",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("content-1"),
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action:          gitalypb.UserCommitFilesActionHeader_MOVE,
									FilePath:        []byte("moved-file"),
									PreviousPath:    []byte("original-file"),
									ExecuteFilemode: true,
									InferContent:    true,
								},
							},
						}),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "moving directory fails",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createDirHeaderRequest("directory"),
						moveFileHeaderRequest("directory", "moved-directory", true),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "move file inferring content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("original-content"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original-content"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("original-file", "moved-file", true),
						actionContentRequest("ignored-content"),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "original-content"},
					},
				},
			},
		},
		{
			desc: "move file with non-existing source",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("non-existing", "destination-file", true),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "move file with already existing destination file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("source-file"),
						createFileHeaderRequest("already-existing"),
						moveFileHeaderRequest("source-file", "already-existing", true),
					},
					indexError: "A file with this name already exists",
				},
			},
		},
		{
			// seems like a bug in the original implementation to allow overwriting a
			// directory
			desc: "move file with already existing destination directory",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("source-file"),
						actionContentRequest("source-content"),
						createDirHeaderRequest("already-existing"),
						moveFileHeaderRequest("source-file", "already-existing", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "already-existing", Content: "source-content"},
					},
				},
			},
		},
		{
			desc: "move file providing content",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("original-content"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original-content"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("original-file", "moved-file", false),
						actionContentRequest("new-content"),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "new-content"},
					},
				},
			},
		},
		{
			desc: "move file normalizes line endings",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("original-file"),
						actionContentRequest("original-content"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "original-file", Content: "original-content"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("original-file", "moved-file", false),
						actionContentRequest("new-content\r\n"),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "moved-file", Content: "new-content\n"},
					},
				},
			},
		},
		{
			desc: "mark non-existing file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("file-1", true),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "mark executable file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						chmodFileHeaderRequest("file-1", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("file-1", true),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1"},
					},
				},
			},
		},
		{
			desc: "mark file executable with directory traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("../file-1", true),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "mark created file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						chmodFileHeaderRequest("file-1", true),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "mark existing file executable",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					repoCreated:   true,
					branchCreated: true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						chmodFileHeaderRequest("file-1", true),
					},
					treeEntries: []testhelper.TreeEntry{
						{Mode: ExecutableMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "move non-existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						moveFileHeaderRequest("non-existing", "should-not-be-created", true),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "move doesn't overwrite a file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						createFileHeaderRequest("file-2"),
						actionContentRequest("content-2"),
						moveFileHeaderRequest("file-1", "file-2", true),
					},
					indexError: "A file with this name already exists",
				},
			},
		},
		{
			desc: "delete non-existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						deleteFileHeaderRequest("non-existing"),
					},
					indexError: "A file with this name doesn't exist",
				},
			},
		},
		{
			desc: "delete file with directory traversal",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						deleteFileHeaderRequest("../file-1"),
					},
					indexError: "Path cannot include directory traversal",
				},
			},
		},
		{
			desc: "delete created file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
						deleteFileHeaderRequest("file-1"),
					},
					branchCreated: true,
					repoCreated:   true,
				},
			},
		},
		{
			desc: "delete existing file",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					branchCreated: true,
					repoCreated:   true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						deleteFileHeaderRequest("file-1"),
					},
				},
			},
		},
		{
			desc: "invalid action",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						actionRequest(&gitalypb.UserCommitFilesAction{
							UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
								Header: &gitalypb.UserCommitFilesActionHeader{
									Action: -1,
								},
							},
						}),
					},
					error: status.Error(codes.Unknown, "NoMethodError: undefined method `downcase' for -1:Integer"),
				},
			},
		},
		{
			desc: "start repository refers to target repository",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					startRepository: &gitalypb.Repository{
						StorageName:  storageName,
						RelativePath: targetRelativePath,
					},
					branchCreated: true,
					repoCreated:   true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "empty target repository with start branch set",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					startBranch:   "master",
					branchCreated: true,
					repoCreated:   true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
		{
			desc: "start repository refers to an empty remote repository",
			steps: []step{
				{
					actions: []*gitalypb.UserCommitFilesRequest{
						createFileHeaderRequest("file-1"),
						actionContentRequest("content-1"),
					},
					startBranch:     "master",
					startRepository: startRepo,
					branchCreated:   true,
					repoCreated:     true,
					treeEntries: []testhelper.TreeEntry{
						{Mode: DefaultMode, Path: "file-1", Content: "content-1"},
					},
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			defer os.RemoveAll(repoPath)
			testhelper.MustRunCommand(t, nil, "git", "init", "--bare", repoPath)

			const branch = "master"

			for i, step := range tc.steps {
				headerRequest := headerRequest(
					testhelper.CreateRepo(t, storageRoot, targetRelativePath),
					testhelper.TestUser,
					branch,
					[]byte("commit message"),
				)
				setAuthorAndEmail(headerRequest, []byte("Author Name"), []byte("author.email@example.com"))

				ctx := ctx
				if step.startRepository != nil {
					ctx = ctxWithServerMetadata
					setStartRepository(headerRequest, step.startRepository)
				}

				if step.startBranch != "" {
					setStartBranchName(headerRequest, []byte(step.startBranch))
				}

				stream, err := client.UserCommitFiles(ctx)
				require.NoError(t, err)
				require.NoError(t, stream.Send(headerRequest))

				for j, action := range step.actions {
					require.NoError(t, stream.Send(action), "step %d, action %d", i+1, j+1)
				}

				resp, err := stream.CloseAndRecv()
				require.Equal(t, step.error, err)
				if step.error != nil {
					continue
				}

				require.Equal(t, step.indexError, resp.IndexError, "step %d", i+1)
				if step.indexError != "" {
					continue
				}

				require.Equal(t, step.branchCreated, resp.BranchUpdate.BranchCreated, "step %d", i+1)
				require.Equal(t, step.repoCreated, resp.BranchUpdate.RepoCreated, "step %d", i+1)
				testhelper.RequireTree(t, repoPath, branch, step.treeEntries)
			}
		})
	}
}

func TestUserCommitFiles_stableCommitID(t *testing.T) {
	testImplementations(t, testUserCommitFilesStableCommitID)
}

func testUserCommitFilesStableCommitID(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repo, repoPath, cleanup := testhelper.InitBareRepo(t)
	defer cleanup()

	for key, values := range testhelper.GitalyServersMetadata(t, serverSocketPath) {
		for _, value := range values {
			ctx = metadata.AppendToOutgoingContext(ctx, key, value)
		}
	}

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)

	headerRequest := headerRequest(repo, testhelper.TestUser, "master", []byte("commit message"))
	setAuthorAndEmail(headerRequest, []byte("Author Name"), []byte("author.email@example.com"))
	setTimestamp(t, headerRequest, time.Unix(12345, 0))
	require.NoError(t, stream.Send(headerRequest))

	require.NoError(t, stream.Send(createFileHeaderRequest("file.txt")))
	require.NoError(t, stream.Send(actionContentRequest("content")))
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, resp.BranchUpdate.CommitId, "4f0ca1fbf05e04dbd5f68d14677034e0afee58ff")
	require.True(t, resp.BranchUpdate.BranchCreated)
	require.True(t, resp.BranchUpdate.RepoCreated)
	testhelper.RequireTree(t, repoPath, "refs/heads/master", []testhelper.TreeEntry{
		{Mode: "100644", Path: "file.txt", Content: "content"},
	})

	commit, err := log.GetCommit(ctx, config.NewLocator(config.Config), repo, "refs/heads/master")
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id:       "4f0ca1fbf05e04dbd5f68d14677034e0afee58ff",
		TreeId:   "541550ddcf8a29bcd80b0800a142a7d47890cfd6",
		Subject:  []byte("commit message"),
		Body:     []byte("commit message"),
		BodySize: 14,
		Author: &gitalypb.CommitAuthor{
			Name:     []byte("Author Name"),
			Email:    []byte("author.email@example.com"),
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     testhelper.TestUser.Name,
			Email:    testhelper.TestUser.Email,
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
	}, commit)
}

func TestSuccessfulUserCommitFilesRequest(t *testing.T) {
	testImplementations(t, testSuccessfulUserCommitFilesRequest)
}

func testSuccessfulUserCommitFilesRequest(t *testing.T, ctx context.Context) {
	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	locator := config.NewLocator(config.Config)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	newRepo, newRepoPath, newRepoCleanupFn := testhelper.InitBareRepo(t)
	defer newRepoCleanupFn()

	filePath := "héllo/wörld"
	authorName := []byte("Jane Doe")
	authorEmail := []byte("janedoe@gitlab.com")
	testCases := []struct {
		desc            string
		repo            *gitalypb.Repository
		repoPath        string
		branchName      string
		repoCreated     bool
		branchCreated   bool
		executeFilemode bool
	}{
		{
			desc:          "existing repo and branch",
			repo:          testRepo,
			repoPath:      testRepoPath,
			branchName:    "feature",
			repoCreated:   false,
			branchCreated: false,
		},
		{
			desc:          "existing repo, new branch",
			repo:          testRepo,
			repoPath:      testRepoPath,
			branchName:    "new-branch",
			repoCreated:   false,
			branchCreated: true,
		},
		{
			desc:          "new repo",
			repo:          newRepo,
			repoPath:      newRepoPath,
			branchName:    "feature",
			repoCreated:   true,
			branchCreated: true,
		},
		{
			desc:            "create executable file",
			repo:            testRepo,
			repoPath:        testRepoPath,
			branchName:      "feature-executable",
			repoCreated:     false,
			branchCreated:   true,
			executeFilemode: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			headerRequest := headerRequest(tc.repo, testhelper.TestUser, tc.branchName, commitFilesMessage)
			setAuthorAndEmail(headerRequest, authorName, authorEmail)

			actionsRequest1 := createFileHeaderRequest(filePath)
			actionsRequest2 := actionContentRequest("My")
			actionsRequest3 := actionContentRequest(" content")
			actionsRequest4 := chmodFileHeaderRequest(filePath, tc.executeFilemode)

			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))
			require.NoError(t, stream.Send(actionsRequest1))
			require.NoError(t, stream.Send(actionsRequest2))
			require.NoError(t, stream.Send(actionsRequest3))
			require.NoError(t, stream.Send(actionsRequest4))

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)
			require.Equal(t, tc.repoCreated, resp.GetBranchUpdate().GetRepoCreated())
			require.Equal(t, tc.branchCreated, resp.GetBranchUpdate().GetBranchCreated())

			headCommit, err := log.GetCommit(ctx, locator, tc.repo, git.Revision(tc.branchName))
			require.NoError(t, err)
			require.Equal(t, authorName, headCommit.Author.Name)
			require.Equal(t, testhelper.TestUser.Name, headCommit.Committer.Name)
			require.Equal(t, authorEmail, headCommit.Author.Email)
			require.Equal(t, testhelper.TestUser.Email, headCommit.Committer.Email)
			require.Equal(t, commitFilesMessage, headCommit.Subject)

			fileContent := testhelper.MustRunCommand(t, nil, "git", "-C", tc.repoPath, "show", headCommit.GetId()+":"+filePath)
			require.Equal(t, "My content", string(fileContent))

			commitInfo := testhelper.MustRunCommand(t, nil, "git", "-C", tc.repoPath, "show", headCommit.GetId())
			expectedFilemode := "100644"
			if tc.executeFilemode {
				expectedFilemode = "100755"
			}
			require.Contains(t, string(commitInfo), fmt.Sprint("new file mode ", expectedFilemode))
		})
	}
}

func TestSuccessfulUserCommitFilesRequestMove(t *testing.T) {
	testImplementations(t, testSuccessfulUserCommitFilesRequestMove)
}

func testSuccessfulUserCommitFilesRequestMove(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	branchName := "master"
	previousFilePath := "README"
	filePath := "NEWREADME"
	authorName := []byte("Jane Doe")
	authorEmail := []byte("janedoe@gitlab.com")

	for i, tc := range []struct {
		content string
		infer   bool
	}{
		{content: "", infer: false},
		{content: "foo", infer: false},
		{content: "", infer: true},
		{content: "foo", infer: true},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
			defer cleanupFn()

			origFileContent := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "show", branchName+":"+previousFilePath)
			headerRequest := headerRequest(testRepo, testhelper.TestUser, branchName, commitFilesMessage)
			setAuthorAndEmail(headerRequest, authorName, authorEmail)
			actionsRequest1 := moveFileHeaderRequest(previousFilePath, filePath, tc.infer)

			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))
			require.NoError(t, stream.Send(actionsRequest1))

			if len(tc.content) > 0 {
				actionsRequest2 := actionContentRequest(tc.content)
				require.NoError(t, stream.Send(actionsRequest2))
			}

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)

			update := resp.GetBranchUpdate()
			require.NotNil(t, update)

			fileContent := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "show", update.CommitId+":"+filePath)

			if tc.infer {
				require.Equal(t, string(origFileContent), string(fileContent))
			} else {
				require.Equal(t, tc.content, string(fileContent))
			}
		})
	}
}

func TestSuccessfulUserCommitFilesRequestForceCommit(t *testing.T) {
	testImplementations(t, testSuccessfulUserCommitFilesRequestForceCommit)
}

func testSuccessfulUserCommitFilesRequestForceCommit(t *testing.T, ctx context.Context) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	authorName := []byte("Jane Doe")
	authorEmail := []byte("janedoe@gitlab.com")
	targetBranchName := "feature"
	startBranchName := []byte("master")

	startBranchCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(startBranchName))
	require.NoError(t, err)

	targetBranchCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(targetBranchName))
	require.NoError(t, err)

	mergeBaseOut := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "merge-base", targetBranchCommit.Id, startBranchCommit.Id)
	mergeBaseID := text.ChompBytes(mergeBaseOut)
	require.NotEqual(t, mergeBaseID, targetBranchCommit.Id, "expected %s not to be an ancestor of %s", targetBranchCommit.Id, startBranchCommit.Id)

	headerRequest := headerRequest(testRepo, testhelper.TestUser, targetBranchName, commitFilesMessage)
	setAuthorAndEmail(headerRequest, authorName, authorEmail)
	setStartBranchName(headerRequest, startBranchName)
	setForce(headerRequest, true)

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(createFileHeaderRequest("TEST.md")))
	require.NoError(t, stream.Send(actionContentRequest("Test")))

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	update := resp.GetBranchUpdate()
	newTargetBranchCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(targetBranchName))
	require.NoError(t, err)

	require.Equal(t, newTargetBranchCommit.Id, update.CommitId)
	require.Equal(t, newTargetBranchCommit.ParentIds, []string{startBranchCommit.Id})
}

func TestSuccessfulUserCommitFilesRequestStartSha(t *testing.T) {
	testImplementations(t, testSuccessfulUserCommitFilesRequestStartSha)
}

func testSuccessfulUserCommitFilesRequestStartSha(t *testing.T, ctx context.Context) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	targetBranchName := "new"

	startCommit, err := log.GetCommit(ctx, locator, testRepo, "master")
	require.NoError(t, err)

	headerRequest := headerRequest(testRepo, testhelper.TestUser, targetBranchName, commitFilesMessage)
	setStartSha(headerRequest, startCommit.Id)

	stream, err := client.UserCommitFiles(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(headerRequest))
	require.NoError(t, stream.Send(createFileHeaderRequest("TEST.md")))
	require.NoError(t, stream.Send(actionContentRequest("Test")))

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	update := resp.GetBranchUpdate()
	newTargetBranchCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(targetBranchName))
	require.NoError(t, err)

	require.Equal(t, newTargetBranchCommit.Id, update.CommitId)
	require.Equal(t, newTargetBranchCommit.ParentIds, []string{startCommit.Id})
}

func TestSuccessfulUserCommitFilesRequestStartShaRemoteRepository(t *testing.T) {
	testImplementations(t, testSuccessfulUserCommitFilesRemoteRepositoryRequest(func(header *gitalypb.UserCommitFilesRequest) {
		setStartSha(header, "1e292f8fedd741b75372e19097c76d327140c312")
	}))
}

func TestSuccessfulUserCommitFilesRequestStartBranchRemoteRepository(t *testing.T) {
	testImplementations(t, testSuccessfulUserCommitFilesRemoteRepositoryRequest(func(header *gitalypb.UserCommitFilesRequest) {
		setStartBranchName(header, []byte("master"))
	}))
}

func testSuccessfulUserCommitFilesRemoteRepositoryRequest(setHeader func(header *gitalypb.UserCommitFilesRequest)) func(*testing.T, context.Context) {
	// Regular table driven test did not work here as there is some state shared in the helpers between the subtests.
	// Running them in different top level tests works, so we use a parameterized function instead to share the code.
	return func(t *testing.T, ctx context.Context) {
		locator := config.NewLocator(config.Config)

		serverSocketPath, stop := runOperationServiceServer(t)
		defer stop()

		client, conn := newOperationClient(t, serverSocketPath)
		defer conn.Close()

		testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
		defer cleanupFn()

		newRepo, _, newRepoCleanupFn := testhelper.InitBareRepo(t)
		defer newRepoCleanupFn()

		for key, values := range testhelper.GitalyServersMetadata(t, serverSocketPath) {
			for _, value := range values {
				ctx = metadata.AppendToOutgoingContext(ctx, key, value)
			}
		}

		targetBranchName := "new"

		startCommit, err := log.GetCommit(ctx, locator, testRepo, "master")
		require.NoError(t, err)

		headerRequest := headerRequest(newRepo, testhelper.TestUser, targetBranchName, commitFilesMessage)
		setHeader(headerRequest)
		setStartRepository(headerRequest, testRepo)

		stream, err := client.UserCommitFiles(ctx)
		require.NoError(t, err)
		require.NoError(t, stream.Send(headerRequest))
		require.NoError(t, stream.Send(createFileHeaderRequest("TEST.md")))
		require.NoError(t, stream.Send(actionContentRequest("Test")))

		resp, err := stream.CloseAndRecv()
		require.NoError(t, err)

		update := resp.GetBranchUpdate()
		newTargetBranchCommit, err := log.GetCommit(ctx, locator, newRepo, git.Revision(targetBranchName))
		require.NoError(t, err)

		require.Equal(t, newTargetBranchCommit.Id, update.CommitId)
		require.Equal(t, newTargetBranchCommit.ParentIds, []string{startCommit.Id})
	}
}

func TestSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature(t *testing.T) {
	testImplementations(t, testSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature)
}

func testSuccessfulUserCommitFilesRequestWithSpecialCharactersInSignature(t *testing.T, ctx context.Context) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.InitBareRepo(t)
	defer cleanupFn()

	targetBranchName := "master"

	testCases := []struct {
		desc   string
		user   *gitalypb.User
		author *gitalypb.CommitAuthor // expected value
	}{
		{
			desc:   "special characters at start and end",
			user:   &gitalypb.User{Name: []byte(".,:;<>\"'\nJane Doe.,:;<>'\"\n"), Email: []byte(".,:;<>'\"\njanedoe@gitlab.com.,:;<>'\"\n"), GlId: testhelper.GlID},
			author: &gitalypb.CommitAuthor{Name: []byte("Jane Doe"), Email: []byte("janedoe@gitlab.com")},
		},
		{
			desc:   "special characters in the middle",
			user:   &gitalypb.User{Name: []byte("Ja<ne\n D>oe"), Email: []byte("ja<ne\ndoe>@gitlab.com"), GlId: testhelper.GlID},
			author: &gitalypb.CommitAuthor{Name: []byte("Jane Doe"), Email: []byte("janedoe@gitlab.com")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			headerRequest := headerRequest(testRepo, tc.user, targetBranchName, commitFilesMessage)
			setAuthorAndEmail(headerRequest, tc.user.Name, tc.user.Email)

			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))

			_, err = stream.CloseAndRecv()
			require.NoError(t, err)

			newCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(targetBranchName))
			require.NoError(t, err)

			require.Equal(t, tc.author.Name, newCommit.Author.Name, "author name")
			require.Equal(t, tc.author.Email, newCommit.Author.Email, "author email")
			require.Equal(t, tc.author.Name, newCommit.Committer.Name, "committer name")
			require.Equal(t, tc.author.Email, newCommit.Committer.Email, "committer email")
		})
	}
}

func TestFailedUserCommitFilesRequestDueToHooks(t *testing.T) {
	testImplementations(t, testFailedUserCommitFilesRequestDueToHooks)
}

func testFailedUserCommitFilesRequestDueToHooks(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	branchName := "feature"
	filePath := "my/file.txt"
	headerRequest := headerRequest(testRepo, testhelper.TestUser, branchName, commitFilesMessage)
	actionsRequest1 := createFileHeaderRequest(filePath)
	actionsRequest2 := actionContentRequest("My content")
	hookContent := []byte("#!/bin/sh\nprintenv | paste -sd ' ' -\nexit 1")

	for _, hookName := range GitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove := testhelper.WriteCustomHook(t, testRepoPath, hookName, hookContent)
			defer remove()

			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(headerRequest))
			require.NoError(t, stream.Send(actionsRequest1))
			require.NoError(t, stream.Send(actionsRequest2))

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)

			require.Contains(t, resp.PreReceiveError, "GL_ID="+testhelper.TestUser.GlId)
			require.Contains(t, resp.PreReceiveError, "GL_USERNAME="+testhelper.TestUser.GlUsername)
		})
	}
}

func TestFailedUserCommitFilesRequestDueToIndexError(t *testing.T) {
	testImplementations(t, testFailedUserCommitFilesRequestDueToIndexError)
}

func testFailedUserCommitFilesRequestDueToIndexError(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc       string
		requests   []*gitalypb.UserCommitFilesRequest
		indexError string
	}{
		{
			desc: "file already exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(testRepo, testhelper.TestUser, "feature", commitFilesMessage),
				createFileHeaderRequest("README.md"),
				actionContentRequest("This file already exists"),
			},
			indexError: "A file with this name already exists",
		},
		{
			desc: "file doesn't exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(testRepo, testhelper.TestUser, "feature", commitFilesMessage),
				chmodFileHeaderRequest("documents/story.txt", true),
			},
			indexError: "A file with this name doesn't exist",
		},
		{
			desc: "dir already exists",
			requests: []*gitalypb.UserCommitFilesRequest{
				headerRequest(testRepo, testhelper.TestUser, "utf-dir", commitFilesMessage),
				actionRequest(&gitalypb.UserCommitFilesAction{
					UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
						Header: &gitalypb.UserCommitFilesActionHeader{
							Action:        gitalypb.UserCommitFilesActionHeader_CREATE_DIR,
							Base64Content: false,
							FilePath:      []byte("héllo"),
						},
					},
				}),
				actionContentRequest("This file already exists, as a directory"),
			},
			indexError: "A directory with this name already exists",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)

			for _, req := range tc.requests {
				require.NoError(t, stream.Send(req))
			}

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)
			require.Equal(t, tc.indexError, resp.GetIndexError())
		})
	}
}

func TestFailedUserCommitFilesRequest(t *testing.T) {
	testImplementations(t, testFailedUserCommitFilesRequest)
}

func testFailedUserCommitFilesRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	branchName := "feature"

	testCases := []struct {
		desc string
		req  *gitalypb.UserCommitFilesRequest
	}{
		{
			desc: "empty Repository",
			req:  headerRequest(nil, testhelper.TestUser, branchName, commitFilesMessage),
		},
		{
			desc: "empty User",
			req:  headerRequest(testRepo, nil, branchName, commitFilesMessage),
		},
		{
			desc: "empty BranchName",
			req:  headerRequest(testRepo, testhelper.TestUser, "", commitFilesMessage),
		},
		{
			desc: "empty CommitMessage",
			req:  headerRequest(testRepo, testhelper.TestUser, branchName, nil),
		},
		{
			desc: "invalid object ID: \"foobar\"",
			req:  setStartSha(headerRequest(testRepo, testhelper.TestUser, branchName, commitFilesMessage), "foobar"),
		},
		{
			desc: "failed to parse signature - Signature cannot have an empty name or email",
			req:  headerRequest(testRepo, &gitalypb.User{}, branchName, commitFilesMessage),
		},
		{
			desc: "failed to parse signature - Signature cannot have an empty name or email",
			req:  headerRequest(testRepo, &gitalypb.User{Name: []byte(""), Email: []byte("")}, branchName, commitFilesMessage),
		},
		{
			desc: "failed to parse signature - Signature cannot have an empty name or email",
			req:  headerRequest(testRepo, &gitalypb.User{Name: []byte(" "), Email: []byte(" ")}, branchName, commitFilesMessage),
		},
		{
			desc: "failed to parse signature - Signature cannot have an empty name or email",
			req:  headerRequest(testRepo, &gitalypb.User{Name: []byte("Jane Doe"), Email: []byte("")}, branchName, commitFilesMessage),
		},
		{
			desc: "failed to parse signature - Signature cannot have an empty name or email",
			req:  headerRequest(testRepo, &gitalypb.User{Name: []byte(""), Email: []byte("janedoe@gitlab.com")}, branchName, commitFilesMessage),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.UserCommitFiles(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(tc.req))

			_, err = stream.CloseAndRecv()
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
		})
	}
}

func headerRequest(repo *gitalypb.Repository, user *gitalypb.User, branchName string, commitMessage []byte) *gitalypb.UserCommitFilesRequest {
	return &gitalypb.UserCommitFilesRequest{
		UserCommitFilesRequestPayload: &gitalypb.UserCommitFilesRequest_Header{
			Header: &gitalypb.UserCommitFilesRequestHeader{
				Repository:      repo,
				User:            user,
				BranchName:      []byte(branchName),
				CommitMessage:   commitMessage,
				StartBranchName: nil,
				StartRepository: nil,
			},
		},
	}
}

func setAuthorAndEmail(headerRequest *gitalypb.UserCommitFilesRequest, authorName, authorEmail []byte) {
	header := getHeader(headerRequest)
	header.CommitAuthorName = authorName
	header.CommitAuthorEmail = authorEmail
}

func setTimestamp(t testing.TB, headerRequest *gitalypb.UserCommitFilesRequest, time time.Time) {
	timestamp, err := ptypes.TimestampProto(time)
	require.NoError(t, err)
	getHeader(headerRequest).Timestamp = timestamp
}

func setStartBranchName(headerRequest *gitalypb.UserCommitFilesRequest, startBranchName []byte) {
	header := getHeader(headerRequest)
	header.StartBranchName = startBranchName
}

func setStartRepository(headerRequest *gitalypb.UserCommitFilesRequest, startRepository *gitalypb.Repository) {
	header := getHeader(headerRequest)
	header.StartRepository = startRepository
}

func setStartSha(headerRequest *gitalypb.UserCommitFilesRequest, startSha string) *gitalypb.UserCommitFilesRequest {
	header := getHeader(headerRequest)
	header.StartSha = startSha

	return headerRequest
}

func setForce(headerRequest *gitalypb.UserCommitFilesRequest, force bool) {
	header := getHeader(headerRequest)
	header.Force = force
}

func getHeader(headerRequest *gitalypb.UserCommitFilesRequest) *gitalypb.UserCommitFilesRequestHeader {
	return headerRequest.UserCommitFilesRequestPayload.(*gitalypb.UserCommitFilesRequest_Header).Header
}

func createDirHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:   gitalypb.UserCommitFilesActionHeader_CREATE_DIR,
				FilePath: []byte(filePath),
			},
		},
	})
}

func createFileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:        gitalypb.UserCommitFilesActionHeader_CREATE,
				Base64Content: false,
				FilePath:      []byte(filePath),
			},
		},
	})
}

func createBase64FileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:        gitalypb.UserCommitFilesActionHeader_CREATE,
				Base64Content: true,
				FilePath:      []byte(filePath),
			},
		},
	})
}

func updateFileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:   gitalypb.UserCommitFilesActionHeader_UPDATE,
				FilePath: []byte(filePath),
			},
		},
	})
}

func updateBase64FileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:        gitalypb.UserCommitFilesActionHeader_UPDATE,
				FilePath:      []byte(filePath),
				Base64Content: true,
			},
		},
	})
}

func chmodFileHeaderRequest(filePath string, executeFilemode bool) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:          gitalypb.UserCommitFilesActionHeader_CHMOD,
				FilePath:        []byte(filePath),
				ExecuteFilemode: executeFilemode,
			},
		},
	})
}

func moveFileHeaderRequest(previousPath, filePath string, infer bool) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:       gitalypb.UserCommitFilesActionHeader_MOVE,
				FilePath:     []byte(filePath),
				PreviousPath: []byte(previousPath),
				InferContent: infer,
			},
		},
	})
}

func deleteFileHeaderRequest(filePath string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Header{
			Header: &gitalypb.UserCommitFilesActionHeader{
				Action:   gitalypb.UserCommitFilesActionHeader_DELETE,
				FilePath: []byte(filePath),
			},
		},
	})
}

func actionContentRequest(content string) *gitalypb.UserCommitFilesRequest {
	return actionRequest(&gitalypb.UserCommitFilesAction{
		UserCommitFilesActionPayload: &gitalypb.UserCommitFilesAction_Content{
			Content: []byte(content),
		},
	})
}

func actionRequest(action *gitalypb.UserCommitFilesAction) *gitalypb.UserCommitFilesRequest {
	return &gitalypb.UserCommitFilesRequest{
		UserCommitFilesRequestPayload: &gitalypb.UserCommitFilesRequest_Action{
			Action: action,
		},
	}
}
