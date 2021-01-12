package repository

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func copyRepoWithNewRemote(t *testing.T, repo *gitalypb.Repository, locator storage.Locator, remote string) (*gitalypb.Repository, string) {
	repoPath, err := locator.GetRepoPath(repo)
	require.NoError(t, err)

	cloneRepo := &gitalypb.Repository{StorageName: repo.GetStorageName(), RelativePath: "fetch-remote-clone.git"}

	clonePath := filepath.Join(testhelper.GitlabTestStoragePath(), "fetch-remote-clone.git")
	require.NoError(t, os.RemoveAll(clonePath))

	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", repoPath, clonePath)

	testhelper.MustRunCommand(t, nil, "git", "-C", clonePath, "remote", "add", remote, repoPath)

	return cloneRepo, clonePath
}

func TestFetchRemoteSuccess(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator, testhelper.WithInternalSocket(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoFetchRemote,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
		defer cleanupFn()

		cloneRepo, cloneRepoPath := copyRepoWithNewRemote(t, testRepo, locator, "my-remote")
		defer func() {
			require.NoError(t, os.RemoveAll(cloneRepoPath))
		}()

		// Ensure there's a new tag to fetch
		testhelper.CreateTag(t, testRepoPath, "testtag", "master", nil)

		// On the first run, TagsChanged will be true for both implementations
		req := &gitalypb.FetchRemoteRequest{Repository: cloneRepo, Remote: "my-remote", Timeout: 120, CheckTagsChanged: true}
		resp, err := client.FetchRemote(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, resp.TagsChanged, true)

		// On the second run, TagsChanged will be false for the Go implementation only
		resp, err = client.FetchRemote(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, resp.TagsChanged, !isFeatureEnabled(ctx, featureflag.GoFetchRemote))

		// Finally, ensure that it returns true if we're asked not to check
		req.CheckTagsChanged = false
		resp, err = client.FetchRemote(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, resp.TagsChanged, true)
	})
}

func TestFetchRemoteFailure(t *testing.T) {
	repo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	serverSocketPath, stop := runRepoServer(t, config.NewLocator(config.Config))
	defer stop()

	const remoteName = "test-repo"
	httpSrv, url := remoteHTTPServer(t, remoteName, httpToken)
	defer httpSrv.Close()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoFetchRemote,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		tests := []struct {
			desc       string
			req        *gitalypb.FetchRemoteRequest
			goCode     codes.Code
			goErrMsg   string
			rubyCode   codes.Code
			rubyErrMsg string
		}{
			{
				desc: "no repository",
				req: &gitalypb.FetchRemoteRequest{
					Repository: nil,
					Remote:     remoteName,
					Timeout:    1000,
				},
				goCode:     codes.InvalidArgument,
				goErrMsg:   "empty Repository",
				rubyCode:   codes.InvalidArgument,
				rubyErrMsg: "",
			},
			{
				desc: "invalid storage",
				req: &gitalypb.FetchRemoteRequest{
					Repository: &gitalypb.Repository{
						StorageName:  "invalid",
						RelativePath: "foobar.git",
					},
					Remote:  remoteName,
					Timeout: 1000,
				},
				// the error text is shortened to only a single word as requests to gitaly done via praefect returns different error messages
				goCode:     codes.InvalidArgument,
				goErrMsg:   "invalid",
				rubyCode:   codes.InvalidArgument,
				rubyErrMsg: "invalid",
			},
			{
				desc: "invalid remote",
				req: &gitalypb.FetchRemoteRequest{
					Repository: repo,
					Remote:     "",
					Timeout:    1000,
				},
				goCode:     codes.InvalidArgument,
				goErrMsg:   `blank or empty "remote"`,
				rubyCode:   codes.Unknown,
				rubyErrMsg: `fatal: no path specified`,
			},
			{
				desc: "invalid remote url: bad format",
				req: &gitalypb.FetchRemoteRequest{
					Repository: repo,
					RemoteParams: &gitalypb.Remote{
						Url:                     "not a url",
						Name:                    remoteName,
						HttpAuthorizationHeader: httpToken,
					},
					Timeout: 1000,
				},
				goCode:     codes.InvalidArgument,
				goErrMsg:   `invalid "remote_params.url"`,
				rubyCode:   codes.InvalidArgument,
				rubyErrMsg: "invalid remote url",
			},
			{
				desc: "invalid remote url: no host",
				req: &gitalypb.FetchRemoteRequest{
					Repository: repo,
					RemoteParams: &gitalypb.Remote{
						Url:                     "/not/a/url",
						Name:                    remoteName,
						HttpAuthorizationHeader: httpToken,
					},
					Timeout: 1000,
				},
				goCode:     codes.InvalidArgument,
				goErrMsg:   `invalid "remote_params.url"`,
				rubyCode:   codes.Unknown,
				rubyErrMsg: "fatal: '/not/a/url' does not appear to be a git repository",
			},
			{
				desc: "no name",
				req: &gitalypb.FetchRemoteRequest{
					Repository: repo,
					RemoteParams: &gitalypb.Remote{
						Name:                    "",
						Url:                     url,
						HttpAuthorizationHeader: httpToken,
					},
					Timeout: 1000,
				},
				goCode:     codes.InvalidArgument,
				goErrMsg:   `blank or empty "remote_params.name"`,
				rubyCode:   codes.Unknown,
				rubyErrMsg: "Rugged::ConfigError: '' is not a valid remote name",
			},
			{
				desc: "not existing repo via http",
				req: &gitalypb.FetchRemoteRequest{
					Repository: repo,
					RemoteParams: &gitalypb.Remote{
						Url:                     httpSrv.URL + "/invalid/repo/path.git",
						Name:                    remoteName,
						HttpAuthorizationHeader: httpToken,
						MirrorRefmaps:           []string{"all_refs"},
					},
					Timeout: 1000,
				},
				goCode:     codes.Unknown,
				goErrMsg:   "invalid/repo/path.git/' not found",
				rubyCode:   codes.Unknown,
				rubyErrMsg: "/invalid/repo/path.git/' not found",
			},
		}
		for _, tc := range tests {
			t.Run(tc.desc, func(t *testing.T) {
				resp, err := client.FetchRemote(ctx, tc.req)
				require.Error(t, err)
				require.Nil(t, resp)

				if isFeatureEnabled(ctx, featureflag.GoFetchRemote) {
					require.Contains(t, err.Error(), tc.goErrMsg)
					testhelper.RequireGrpcError(t, err, tc.goCode)
				} else {
					require.Contains(t, err.Error(), tc.rubyErrMsg)
					testhelper.RequireGrpcError(t, err, tc.rubyCode)
				}
			})
		}
	})
}

const (
	httpToken = "ABCefg0999182"
)

func remoteHTTPServer(t *testing.T, repoName, httpToken string) (*httptest.Server, string) {
	b, err := ioutil.ReadFile("testdata/advertise.txt")
	require.NoError(t, err)

	s := httptest.NewServer(
		// https://github.com/git/git/blob/master/Documentation/technical/http-protocol.txt
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.String() != fmt.Sprintf("/%s.git/info/refs?service=git-upload-pack", repoName) {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			if httpToken != "" && r.Header.Get("Authorization") != httpToken {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			_, err = w.Write(b)
			assert.NoError(t, err)
		}),
	)

	return s, fmt.Sprintf("%s/%s.git", s.URL, repoName)
}

func getRefnames(t *testing.T, repoPath string) []string {
	result := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "for-each-ref", "--format", "%(refname:lstrip=2)")
	return strings.Split(text.ChompBytes(result), "\n")
}

func TestFetchRemoteOverHTTP(t *testing.T) {
	serverSocketPath, stop := runRepoServer(t, config.NewLocator(config.Config), testhelper.WithInternalSocket(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoFetchRemote,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testCases := []struct {
			description string
			httpToken   string
			remoteURL   string
		}{
			{
				description: "with http token",
				httpToken:   httpToken,
			},
			{
				description: "without http token",
				httpToken:   "",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.description, func(t *testing.T) {
				forkedRepo, forkedRepoPath, forkedRepoCleanup := testhelper.NewTestRepo(t)
				defer forkedRepoCleanup()

				s, remoteURL := remoteHTTPServer(t, "my-repo", tc.httpToken)
				defer s.Close()

				req := &gitalypb.FetchRemoteRequest{
					Repository: forkedRepo,
					RemoteParams: &gitalypb.Remote{
						Url:                     remoteURL,
						Name:                    "geo",
						HttpAuthorizationHeader: tc.httpToken,
						MirrorRefmaps:           []string{"all_refs"},
					},
					Timeout: 1000,
				}
				if tc.remoteURL != "" {
					req.RemoteParams.Url = s.URL + tc.remoteURL
				}

				refs := getRefnames(t, forkedRepoPath)
				require.True(t, len(refs) > 1, "the advertisement.txt should have deleted all refs except for master")

				_, err := client.FetchRemote(ctx, req)
				require.NoError(t, err)

				refs = getRefnames(t, forkedRepoPath)

				require.Len(t, refs, 1)
				assert.Equal(t, "master", refs[0])
			})
		}
	})
}

func TestFetchRemoteOverHTTPWithRedirect(t *testing.T) {
	serverSocketPath, stop := runRepoServer(t, config.NewLocator(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/info/refs?service=git-upload-pack", r.URL.String())
			http.Redirect(w, r, "/redirect_url", http.StatusSeeOther)
		}),
	)
	defer s.Close()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoFetchRemote,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testRepo, _, cleanup := testhelper.NewTestRepo(t)
		defer cleanup()

		req := &gitalypb.FetchRemoteRequest{
			Repository:   testRepo,
			RemoteParams: &gitalypb.Remote{Url: s.URL, Name: "geo"},
			Timeout:      1000,
		}

		_, err := client.FetchRemote(ctx, req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "The requested URL returned error: 303")
	})
}

func TestFetchRemoteOverHTTPWithTimeout(t *testing.T) {
	serverSocketPath, stop := runRepoServer(t, config.NewLocator(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	s := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/info/refs?service=git-upload-pack", r.URL.String())
			time.Sleep(2 * time.Second)
			http.Error(w, "", http.StatusNotFound)
		}),
	)
	defer s.Close()

	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoFetchRemote,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testRepo, _, cleanup := testhelper.NewTestRepo(t)
		defer cleanup()

		req := &gitalypb.FetchRemoteRequest{
			Repository:   testRepo,
			RemoteParams: &gitalypb.Remote{Url: s.URL, Name: "geo"},
			Timeout:      1,
		}

		_, err := client.FetchRemote(ctx, req)
		require.Error(t, err)

		if isFeatureEnabled(ctx, featureflag.GoFetchRemote) {
			require.Contains(t, err.Error(), "fetch remote: signal: terminated")
		} else {
			require.Contains(t, err.Error(), "failed: Timed out")
		}
	})
}

func isFeatureEnabled(ctx context.Context, flag featureflag.FeatureFlag) bool {
	md, _ := metadata.FromOutgoingContext(ctx)
	return featureflag.IsEnabled(metadata.NewIncomingContext(ctx, md), flag)
}
