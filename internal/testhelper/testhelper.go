package testhelper

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	log "github.com/Sirupsen/logrus"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// TestRelativePath is the path inside its storage of the gitlab-test repo
const TestRelativePath = "gitlab-test.git"

// MustReadFile returns the content of a file or fails at once.
func MustReadFile(t *testing.T, filename string) []byte {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	return content
}

// GitlabTestStoragePath returns the storage path to the gitlab-test repo.
func GitlabTestStoragePath() string {
	// If TEST_REPO_STORAGE_PATH has been set (by the Makefile) then use that
	testRepoPath := os.Getenv("TEST_REPO_STORAGE_PATH")
	if testRepoPath != "" {
		testRepoPathAbs, err := filepath.Abs(testRepoPath)
		if err != nil {
			log.Fatal(err)
		}

		return testRepoPathAbs
	}

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("Could not get caller info")
	}
	return path.Join(path.Dir(currentFile), "testdata/data")
}

func configureTestStorage() {
	config.Config.Storages = []config.Storage{
		{Name: "default", Path: GitlabTestStoragePath()},
	}
}

func testRepoValid(repo *pb.Repository) bool {
	storagePath, _ := config.StoragePath(repo.GetStorageName())
	if _, err := os.Stat(path.Join(storagePath, repo.RelativePath, "objects")); err != nil {
		return false
	}

	return true
}

// TestRepository returns the `Repository` object for the gitlab-test repo.
// Tests should be calling this function instead of cloning the repo themselves.
// Tests that involve modifications to the repo should copy/clone the repo
// via the `Repository` returned from this function.
func TestRepository() *pb.Repository {
	configureTestStorage()
	repo := &pb.Repository{StorageName: "default", RelativePath: TestRelativePath}

	if !testRepoValid(repo) {
		log.Fatalf("Test repo not found, did you run `make prepare-tests`?")
	}

	return repo
}

// AssertGrpcError asserts the passed err is of the same code as expectedCode. Optionally, it can
// assert the error contains the text of containsText if the latter is not an empty string.
func AssertGrpcError(t *testing.T, err error, expectedCode codes.Code, containsText string) {
	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	// Check that the code matches
	if code := grpc.Code(err); code != expectedCode {
		t.Fatalf("Expected an error with code %v, got %v. The error was %v", expectedCode, code, err)
	}

	if containsText != "" && !strings.Contains(err.Error(), containsText) {
		t.Fatal(err)
	}
}

// MustRunCommand runs a command with an optional standard input and returns the standard output, or fails.
func MustRunCommand(t *testing.T, stdin io.Reader, name string, args ...string) []byte {
	cmd := exec.Command(name, args...)
	if stdin != nil {
		cmd.Stdin = stdin
	}

	output, err := cmd.Output()
	if err != nil {
		stderr := err.(*exec.ExitError).Stderr
		if t == nil {
			log.Print(name, args)
			log.Printf("%s", stderr)
			log.Fatal(err)
		} else {
			t.Log(name, args)
			t.Logf("%s", stderr)
			t.Fatal(err)
		}
	}

	return output
}

// AuthorsEqual tests if two `CommitAuthor`s are equal
func AuthorsEqual(a *pb.CommitAuthor, b *pb.CommitAuthor) bool {
	return bytes.Equal(a.Name, b.Name) &&
		bytes.Equal(a.Email, b.Email) &&
		a.Date.Seconds == b.Date.Seconds
}

// CommitsEqual tests if two `GitCommit`s are equal
func CommitsEqual(a *pb.GitCommit, b *pb.GitCommit) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.Id == b.Id &&
		bytes.Equal(a.Subject, b.Subject) &&
		bytes.Equal(a.Body, b.Body) &&
		AuthorsEqual(a.Author, b.Author) &&
		AuthorsEqual(a.Committer, b.Committer) &&
		reflect.DeepEqual(a.ParentIds, b.ParentIds)
}

// FindLocalBranchCommitAuthorsEqual tests if two `FindLocalBranchCommitAuthor`s are equal
func FindLocalBranchCommitAuthorsEqual(a *pb.FindLocalBranchCommitAuthor, b *pb.FindLocalBranchCommitAuthor) bool {
	return bytes.Equal(a.Name, b.Name) &&
		bytes.Equal(a.Email, b.Email) &&
		a.Date.Seconds == b.Date.Seconds
}

// FindLocalBranchResponsesEqual tests if two `FindLocalBranchResponse`s are equal
func FindLocalBranchResponsesEqual(a *pb.FindLocalBranchResponse, b *pb.FindLocalBranchResponse) bool {
	return a.CommitId == b.CommitId &&
		bytes.Equal(a.CommitSubject, b.CommitSubject) &&
		FindLocalBranchCommitAuthorsEqual(a.CommitAuthor, b.CommitAuthor) &&
		FindLocalBranchCommitAuthorsEqual(a.CommitCommitter, b.CommitCommitter)
}

// GetTemporaryGitalySocketFileName will return a unique, useable socket file name
func GetTemporaryGitalySocketFileName() string {
	tmpfile, err := ioutil.TempFile("", "gitaly.socket.")
	if err != nil {
		// No point in handling this error, panic
		panic(err)
	}

	name := tmpfile.Name()
	tmpfile.Close()
	os.Remove(name)

	return name
}

// ConfigureRuby configures Ruby settings for test purposes at run time.
func ConfigureRuby() {
	if dir := os.Getenv("GITALY_TEST_RUBY_DIR"); len(dir) > 0 {
		// Sometimes runtime.Caller is unreliable. This environment variable provides a bypass.
		config.Config.Ruby.Dir = dir
		return
	}

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("Could not get caller info")
	}
	config.Config.Ruby.Dir = path.Join(path.Dir(currentFile), "../../ruby")
}

// NewTestGrpcServer creates a GRPC Server for testing purposes
func NewTestGrpcServer(t *testing.T, streamInterceptors []grpc.StreamServerInterceptor, unaryInterceptors []grpc.UnaryServerInterceptor) *grpc.Server {
	logger := NewTestLogger(t)
	logrusEntry := log.NewEntry(logger).WithField("test", t.Name())

	streamInterceptors = append([]grpc.StreamServerInterceptor{grpc_logrus.StreamServerInterceptor(logrusEntry)}, streamInterceptors...)
	unaryInterceptors = append([]grpc.UnaryServerInterceptor{grpc_logrus.UnaryServerInterceptor(logrusEntry)}, unaryInterceptors...)
	return grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	)
}
