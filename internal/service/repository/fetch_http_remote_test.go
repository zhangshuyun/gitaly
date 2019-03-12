package repository

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

const (
	httpToken = "ABCefg0999182"
)

func remoteHTTPServer(t *testing.T, repoName, httpToken string) (*httptest.Server, string) {
	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.String() == fmt.Sprintf("/%s.git/info/refs?service=git-upload-pack", repoName) {
				if r.Header.Get("Authorization") != httpToken {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
				w.WriteHeader(http.StatusOK)

				b, err := ioutil.ReadFile("testdata/advertise.txt")
				require.NoError(t, err)
				w.Write(b)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		}),
	)

	return s, fmt.Sprintf("%s/%s.git", s.URL, repoName)
}

func TestFetchHTTPRemoteOverHTTP(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	forkedRepo, forkedRepoPath, forkedRepoCleanup := testhelper.NewTestRepo(t)
	defer forkedRepoCleanup()

	repoName := "my-repo"

	_, remoteURL := remoteHTTPServer(t, repoName, httpToken)

	req := &gitalypb.FetchHTTPRemoteRequest{
		Repository: forkedRepo,
		Remote: &gitalypb.Remote{
			Url:                     remoteURL,
			Name:                    "geo",
			HttpAuthorizationHeader: httpToken,
		},
		Timeout: 1000,
	}

	refs := getRefnames(t, forkedRepoPath)
	require.True(t, len(refs) > 1)

	_, err := client.FetchHTTPRemote(ctx, req)
	require.NoError(t, err)

	refs = getRefnames(t, forkedRepoPath)
	require.Len(t, refs, 1)
	assert.Equal(t, "master", refs[0])
}

func getRefnames(t *testing.T, repoPath string) []string {
	result := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "for-each-ref", "--format", "%(refname:lstrip=2)")
	return strings.Split(string(bytes.TrimRight(result, "\n")), "\n")
}

func TestFetchHTTPRemoteValidationError(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	forkedRepo, _, forkRepoCleanup := getForkDestination(t)
	defer forkRepoCleanup()

	testCases := []struct {
		description string
		request     gitalypb.FetchHTTPRemoteRequest
	}{
		{
			description: "missing repository",
			request: gitalypb.FetchHTTPRemoteRequest{
				Repository: nil,
				Remote:     &gitalypb.Remote{},
			},
		},
		{
			description: "missing remote url",
			request: gitalypb.FetchHTTPRemoteRequest{
				Repository: forkedRepo,
				Remote: &gitalypb.Remote{
					Name: "geo",
					Url:  "",
				},
			},
		},
		{
			description: "missing remote name",
			request: gitalypb.FetchHTTPRemoteRequest{
				Repository: forkedRepo,
				Remote: &gitalypb.Remote{
					Name: "",
					Url:  "https://www.gitlab.com",
				},
			},
		},
		{
			description: "bad remote url",
			request: gitalypb.FetchHTTPRemoteRequest{
				Repository: forkedRepo,
				Remote: &gitalypb.Remote{
					Name: "geo",
					Url:  "not a real url",
				},
			},
		},
		{
			description: "a valid file url",
			request: gitalypb.FetchHTTPRemoteRequest{
				Repository: forkedRepo,
				Remote: &gitalypb.Remote{
					Name: "geo",
					Url:  "file://some/path",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			_, err := client.FetchHTTPRemote(ctx, &tc.request)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}
