package router

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
)

const testRepoRoot = "testdata/data"
const testRepo = "group/test.git"

func TestMain(m *testing.M) {
	source := "https://gitlab.com/gitlab-org/gitlab-test.git"
	clonePath := path.Join(testRepoRoot, testRepo)
	if _, err := os.Stat(clonePath); err != nil {
		testCmd := exec.Command("git", "clone", "--bare", source, clonePath)
		testCmd.Stdout = os.Stdout
		testCmd.Stderr = os.Stderr

		if err := testCmd.Run(); err != nil {
			log.Printf("Test setup: failed to run %v", testCmd)
			os.Exit(-1)
		}
	}

	os.Exit(func() int {
		return m.Run()
	}())
}

func TestSuccessfulUploadPackRequest(t *testing.T) {
	recorder := httptest.NewRecorder()

	resource := "/projects/1/git-http/info-refs/upload-pack"
	req, err := http.NewRequest("GET", resource, &bytes.Buffer{})
	if err != nil {
		t.Fatal("Failed creating a request to %s", resource)
	}

	req.Header.Add("Gitaly-Repo-Path", path.Join(testRepoRoot, testRepo))
	req.Header.Add("Gitaly-GL-Id", "user-123")

	NewRouter().ServeHTTP(recorder, req)

	if recorder.Code != 200 {
		t.Errorf("GET %q: expected 200, got %d", resource, recorder.Code)
	}

	response := recorder.Body.String()
	assertGitRefAdvertisement(t, resource, response, "001e# service=git-upload-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"00416f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9 refs/tags/v1.0.0^{}",
	})
}

func TestSuccessfulReceivePackRequest(t *testing.T) {
	recorder := httptest.NewRecorder()

	resource := "/projects/1/git-http/info-refs/receive-pack"
	req, err := http.NewRequest("GET", resource, &bytes.Buffer{})
	if err != nil {
		t.Fatal("Failed creating a request to %s", resource)
	}

	req.Header.Add("Gitaly-Repo-Path", path.Join(testRepoRoot, testRepo))
	req.Header.Add("Gitaly-GL-Id", "user-123")

	NewRouter().ServeHTTP(recorder, req)

	if recorder.Code != 200 {
		t.Errorf("GET %q: expected 200, got %d", resource, recorder.Code)
	}

	response := recorder.Body.String()
	assertGitRefAdvertisement(t, resource, response, "001f# service=git-receive-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"003e8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b refs/tags/v1.1.0",
	})
}

func TestFailedUploadPackRequestDueToMissingHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()

	resource := "/projects/1/git-http/info-refs/upload-pack"

	req, err := http.NewRequest("GET", resource, &bytes.Buffer{})
	if err != nil {
		t.Fatal("Failed creating a request to %s", resource)
	}
	NewRouter().ServeHTTP(recorder, req)

	expected := 500
	if recorder.Code != expected {
		t.Errorf("GET %q: expected %d, got %d", resource, expected, recorder.Code)
	}
}

func assertGitRefAdvertisement(t *testing.T, requestPath, responseBody string, firstLine, lastLine string, middleLines []string) {
	responseLines := strings.Split(responseBody, "\n")

	if responseLines[0] != firstLine {
		t.Errorf("GET %q: expected response first line to be %q, found %q", requestPath, firstLine, responseLines[0])
	}

	lastIndex := len(responseLines) - 1
	if responseLines[lastIndex] != lastLine {
		t.Errorf("GET %q: expected response last line to be %q, found %q", requestPath, lastLine, responseLines[lastIndex])
	}

	for _, ref := range middleLines {
		if !strings.Contains(responseBody, ref) {
			t.Errorf("GET %q: expected response to contain %q, found none", requestPath, ref)
		}
	}
}
