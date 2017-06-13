package testhelper

import (
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"testing"

	log "github.com/Sirupsen/logrus"

	"path/filepath"

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
	// If TEST_REPO_LOCATION has been set (by the Makefile) then use that
	testRepoPath := os.Getenv("TEST_REPO_LOCATION")
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

// GitlabTestRepoPath returns the path to gitlab-test repo.
// Tests should be calling this function instead of cloning the repo themselves.
// Tests that involve modifications to the repo should copy/clone the repo
// via the path returned from this function.
func GitlabTestRepoPath() string {
	clonePath := path.Join(GitlabTestStoragePath(), TestRelativePath)
	if _, err := os.Stat(path.Join(clonePath, "objects")); err != nil {
		log.Fatal("Test repo not found, did you run `make test`?")
	}

	return clonePath
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
		t.Log(name, args)
		t.Fatal(err)
	}

	return output
}
