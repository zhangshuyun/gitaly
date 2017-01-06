package handler

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
)

func TestGetPostReceive(t *testing.T) {
	recorder := httptest.NewRecorder()
	data := url.Values{}
	data.Set("project", "foo/bar.git")
	data.Set("changes", "0000000000000000000000000000000000000000 92d0970eefd7acb6d548878925ce2208cfe2d2ec refs/heads/branch4")

	req, err := http.NewRequest("POST", "/post-receive", bytes.NewBufferString(data.Encode()))
	if err != nil {
		t.Fatal("Creating 'POST /post-receive' request failed!")
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	http.HandlerFunc(PostReceive).ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatal("Server error: Returned ", recorder.Code, " instead of ", http.StatusOK)
	}
}
