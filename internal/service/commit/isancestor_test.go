package commit

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

func TestCommitIsAncestorFailure(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	queries := []struct {
		Request   *pb.CommitIsAncestorRequest
		ErrorCode codes.Code
		ErrMsg    string
	}{
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: nil,
				AncestorId: "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				ChildId:    "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			},
			ErrorCode: codes.InvalidArgument,
			ErrMsg:    "Expected to throw invalid argument got: %s",
		},
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: testRepo,
				AncestorId: "",
				ChildId:    "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			},
			ErrorCode: codes.InvalidArgument,
			ErrMsg:    "Expected to throw invalid argument got: %s",
		},
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: testRepo,
				AncestorId: "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				ChildId:    "",
			},
			ErrorCode: codes.InvalidArgument,
			ErrMsg:    "Expected to throw invalid argument got: %s",
		},
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: &pb.Repository{StorageName: "default", RelativePath: "fake-path"},
				AncestorId: "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				ChildId:    "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
			},
			ErrorCode: codes.NotFound,
			ErrMsg:    "Expected to throw internal got: %s",
		},
	}

	for _, v := range queries {
		t.Run(fmt.Sprintf("%v", v.Request), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if _, err := client.CommitIsAncestor(ctx, v.Request); err == nil {
				t.Error("Expected to throw an error")
			} else if grpc.Code(err) != v.ErrorCode {
				t.Errorf(v.ErrMsg, err)
			}
		})
	}
}

func TestCommitIsAncestorSuccess(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	queries := []struct {
		Request  *pb.CommitIsAncestorRequest
		Response bool
		ErrMsg   string
	}{
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: testRepo,
				AncestorId: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
				ChildId:    "372ab6950519549b14d220271ee2322caa44d4eb",
			},
			Response: true,
			ErrMsg:   "Expected commit to be ancestor",
		},
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: testRepo,
				AncestorId: "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				ChildId:    "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e",
			},
			Response: false,
			ErrMsg:   "Expected commit not to be ancestor",
		},
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: testRepo,
				AncestorId: "1234123412341234123412341234123412341234",
				ChildId:    "b83d6e391c22777fca1ed3012fce84f633d7fed0",
			},
			Response: false,
			ErrMsg:   "Expected invalid commit to not be ancestor",
		},
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: testRepo,
				AncestorId: "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				ChildId:    "gitaly-stuff",
			},
			Response: true,
			ErrMsg:   "Expected `b83d6e391c22777fca1ed3012fce84f633d7fed0` to be ancestor of `gitaly-stuff`",
		},
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: testRepo,
				AncestorId: "gitaly-stuff",
				ChildId:    "master",
			},
			Response: false,
			ErrMsg:   "Expected branch `gitaly-stuff` not to be ancestor of `master`",
		},
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: testRepo,
				AncestorId: "refs/tags/v1.0.0",
				ChildId:    "refs/tags/v1.1.0",
			},
			Response: true,
			ErrMsg:   "Expected tag `v1.0.0` to be ancestor of `v1.1.0`",
		},
		{
			Request: &pb.CommitIsAncestorRequest{
				Repository: testRepo,
				AncestorId: "refs/tags/v1.1.0",
				ChildId:    "refs/tags/v1.0.0",
			},
			Response: false,
			ErrMsg:   "Expected branch `v1.1.0` not to be ancestor of `v1.0.0`",
		},
	}

	for _, v := range queries {
		t.Run(fmt.Sprintf("%v", v.Request), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitIsAncestor(ctx, v.Request)
			if err != nil {
				t.Fatalf("CommitIsAncestor threw error unexpectedly: %v", err)
			}

			response := c.GetValue()
			if response != v.Response {
				t.Errorf(v.ErrMsg)
			}
		})
	}
}

func TestSuccessfulIsAncestorRequestWithAltGitObjectDirs(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	storagePath := testhelper.GitlabTestStoragePath()
	testRepoPath := path.Join(storagePath, testRepo.RelativePath)
	testRepoCopyPath := path.Join(storagePath, "is-ancestor-alt-test-repo")
	altObjectsPath := path.Join(testRepoCopyPath, ".git/alt-objects")
	gitObjectEnv := []string{
		fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", altObjectsPath),
		fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", path.Join(testRepoCopyPath, ".git/objects")),
	}

	testhelper.MustRunCommand(t, nil, "git", "clone", testRepoPath, testRepoCopyPath)
	defer os.RemoveAll(testRepoCopyPath)

	if err := os.Mkdir(altObjectsPath, 0777); err != nil {
		t.Fatal(err)
	}

	previousHead := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoCopyPath, "show", "--format=format:%H", "--no-patch", "HEAD")

	cmd := exec.Command("git", "-C", testRepoCopyPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", "An empty commit")
	cmd.Env = gitObjectEnv
	if _, err := cmd.Output(); err != nil {
		stderr := err.(*exec.ExitError).Stderr // XXX
		t.Fatalf("%s", stderr)
	}

	cmd = exec.Command("git", "-C", testRepoCopyPath, "show", "--format=format:%H", "--no-patch", "HEAD")
	cmd.Env = gitObjectEnv
	currentHead, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		desc    string
		altDirs []string
		result  bool
	}{
		{
			desc:    "present GIT_ALTERNATE_OBJECT_DIRECTORIES",
			altDirs: []string{altObjectsPath},
			result:  true,
		},
		{
			desc:    "empty GIT_ALTERNATE_OBJECT_DIRECTORIES",
			altDirs: []string{},
			result:  false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &pb.CommitIsAncestorRequest{
				Repository: &pb.Repository{
					StorageName:                   testRepo.StorageName,
					RelativePath:                  testRepo.RelativePath,
					GitAlternateObjectDirectories: testCase.altDirs,
				},
				AncestorId: string(previousHead),
				ChildId:    string(currentHead),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response, err := client.CommitIsAncestor(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(t, testCase.result, response.Value)
		})
	}
}
