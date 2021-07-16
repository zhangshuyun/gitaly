package gitlab

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

type postReceiveRequest struct {
	GLRepository string   `json:"gl_repository,omitempty"`
	Identifier   string   `json:"identifier,omitempty"`
	Changes      string   `json:"changes,omitempty"`
	PushOptions  []string `json:"push_options,omitempty"`
}

// TestAllowedVerifyParams uses client cert fixtures to test TLS connections. To
// regenerate these certs, run `go generate access_test.go`.
//go:generate openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -out testdata/certs/server.crt -keyout testdata/certs/server.key -subj "/C=US/ST=California/L=San Francisco/O=GitLab/OU=GitLab-Shell/CN=localhost" -addext "subjectAltName = IP:127.0.0.1"
func TestAccess_verifyParams(t *testing.T) {
	user, password := "user", "password"
	secretToken := "topsecret"
	glID, glRepository := "key-123", "repo-1"

	_, repo, repoPath := testcfg.BuildWithRepo(t)
	changes := "changes1\nchanges2\nchanges3"
	protocol := "protocol"

	repo.GitObjectDirectory = "object/dir"
	repo.GitAlternateObjectDirectories = []string{"alt/object/dir1", "alt/object/dir2"}

	gitObjectDirFull := filepath.Join(repoPath, repo.GitObjectDirectory)
	var gitAlternateObjectDirsFull []string

	for _, gitAlternateObjectDirRel := range repo.GitAlternateObjectDirectories {
		gitAlternateObjectDirsFull = append(gitAlternateObjectDirsFull, filepath.Join(repoPath, gitAlternateObjectDirRel))
	}

	tempDir := testhelper.TempDir(t)
	WriteShellSecretFile(t, tempDir, secretToken)

	secretFilePath := filepath.Join(tempDir, ".gitlab_shell_secret")

	serverURL, cleanup := NewTestServer(t, TestServerOptions{
		User:                        user,
		Password:                    password,
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                glRepository,
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    protocol,
		GitPushOptions:              nil,
		GitObjectDir:                gitObjectDirFull,
		GitAlternateObjectDirs:      gitAlternateObjectDirsFull,
		RepoPath:                    repoPath,
		ClientCACertPath:            "testdata/certs/server.crt",
		ServerCertPath:              "testdata/certs/server.crt",
		ServerKeyPath:               "testdata/certs/server.key",
	})
	defer cleanup()

	c, err := NewHTTPClient(config.Gitlab{
		URL:        serverURL,
		SecretFile: secretFilePath,
		HTTPSettings: config.HTTPSettings{
			User:     user,
			Password: password,
			CAFile:   "testdata/certs/server.crt",
		},
	}, config.TLS{
		CertPath: "testdata/certs/server.crt",
		KeyPath:  "testdata/certs/server.key",
	}, prometheus.Config{})
	require.NoError(t, err)

	badRepo := proto.Clone(repo).(*gitalypb.Repository)
	badRepo.GitObjectDirectory = filepath.Join(repoPath, "bad/object/directory")

	testCases := []struct {
		desc                                  string
		repo                                  *gitalypb.Repository
		glRepository, glID, protocol, changes string
		allowed                               bool
	}{
		{
			desc:         "success",
			repo:         repo,
			glRepository: glRepository,
			glID:         glID,
			protocol:     protocol,
			changes:      changes,
			allowed:      true,
		},
		{
			desc:         "repo with bad quarantine directories",
			repo:         badRepo,
			glRepository: glRepository,
			glID:         glID,
			protocol:     protocol,
			changes:      changes,
			allowed:      false,
		},
	}

	for _, tc := range testCases {
		allowed, _, err := c.Allowed(context.Background(), AllowedParams{
			RepoPath:                      tc.repo.RelativePath,
			GitObjectDirectory:            tc.repo.GitObjectDirectory,
			GitAlternateObjectDirectories: tc.repo.GitAlternateObjectDirectories,
			GLRepository:                  tc.glRepository,
			GLID:                          tc.glID,
			GLProtocol:                    tc.protocol,
			Changes:                       tc.changes,
		})
		require.NoError(t, err)
		require.Equal(t, tc.allowed, allowed)
	}
}

func TestAccess_escapedAndRelativeURLs(t *testing.T) {
	user, password := "user", "password"
	secretToken := "topsecret"
	glID, glRepository := "key-123", "repo-1"

	_, repo, repoPath := testcfg.BuildWithRepo(t)
	changes := "changes1\nchanges2\nchanges3"
	protocol := "protocol"

	repo.GitObjectDirectory = "object/dir"
	repo.GitAlternateObjectDirectories = []string{"alt/object/dir1", "alt/object/dir2"}

	gitObjectDirFull := filepath.Join(repoPath, repo.GitObjectDirectory)
	var gitAlternateObjectDirsFull []string

	for _, gitAlternateObjectDirRel := range repo.GitAlternateObjectDirectories {
		gitAlternateObjectDirsFull = append(gitAlternateObjectDirsFull, filepath.Join(repoPath, gitAlternateObjectDirRel))
	}

	tempDir := testhelper.TempDir(t)
	WriteShellSecretFile(t, tempDir, secretToken)

	secretFilePath := filepath.Join(tempDir, ".gitlab_shell_secret")

	testCases := []struct {
		desc            string
		escaped         bool
		relativeURLRoot string
		unixSocket      bool
	}{
		{
			desc: "unescaped URL",
		},
		{
			desc:    "escaped URL",
			escaped: true,
		},
		{
			desc:       "UNIX socket with no relative root",
			unixSocket: true,
		},
		{
			desc:            "UNIX socket with / root",
			unixSocket:      true,
			relativeURLRoot: "/",
		},
		{
			desc:            "UNIX socket with /gitlab root",
			unixSocket:      true,
			relativeURLRoot: "/gitlab",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			serverURL, cleanup := NewTestServer(t, TestServerOptions{
				User:                        user,
				Password:                    password,
				SecretToken:                 secretToken,
				GLID:                        glID,
				GLRepository:                glRepository,
				Changes:                     changes,
				PostReceiveCounterDecreased: true,
				Protocol:                    protocol,
				GitPushOptions:              nil,
				GitObjectDir:                gitObjectDirFull,
				GitAlternateObjectDirs:      gitAlternateObjectDirsFull,
				RepoPath:                    repoPath,
				RelativeURLRoot:             tc.relativeURLRoot,
				UnixSocket:                  tc.unixSocket,
			})
			defer cleanup()

			if tc.escaped {
				serverURL = url.PathEscape(serverURL)
			}

			c, err := NewHTTPClient(config.Gitlab{
				URL:             serverURL,
				RelativeURLRoot: tc.relativeURLRoot,
				SecretFile:      secretFilePath,
				HTTPSettings: config.HTTPSettings{
					User:     user,
					Password: password,
				},
			}, config.TLS{}, prometheus.Config{})
			require.NoError(t, err)
			allowed, _, err := c.Allowed(context.Background(), AllowedParams{
				RepoPath:                      repo.RelativePath,
				GitObjectDirectory:            repo.GitObjectDirectory,
				GitAlternateObjectDirectories: repo.GitAlternateObjectDirectories,
				GLID:                          glID,
				GLRepository:                  glRepository,
				GLProtocol:                    protocol,
				Changes:                       changes,
			})
			require.NoError(t, err)
			require.True(t, allowed)
		})
	}
}

func TestAccess_allowedResponseHandling(t *testing.T) {
	_, repo, repoPath := testcfg.BuildWithRepo(t)

	// set git quarantine directories
	gitObjectDir := filepath.Join(repoPath, "quarantine", "object", "dir")
	repo.GitObjectDirectory = gitObjectDir
	gitAltObjectDir := filepath.Join(repoPath, "objects")
	repo.GitAlternateObjectDirectories = []string{gitAltObjectDir}

	tempDir := testhelper.TempDir(t)
	WriteShellSecretFile(t, tempDir, "secret_token")

	secretFilePath := filepath.Join(tempDir, ".gitlab_shell_secret")

	testCases := []struct {
		desc           string
		allowedHandler func(w http.ResponseWriter, r *http.Request)
		allowed        bool
		errMsg         string
	}{

		{
			desc: "allowed",
			allowedHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`{"status": true}`))
				require.NoError(t, err)
			},
			allowed: true,
			errMsg:  "",
		},

		{
			desc: "not allowed",
			allowedHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`{"status": false, "message": "this change is not allowed"}`))
				require.NoError(t, err)
			},
			allowed: false,
			errMsg:  "this change is not allowed",
		},
		{
			desc: "bad content type in response",
			allowedHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "bad mime type")
				w.WriteHeader(http.StatusOK)
			},
			allowed: false,
			errMsg:  "unsupported content type",
		},
		{
			desc: "internal server error",
			allowedHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, err := w.Write([]byte(`{"status": true}`))
				require.NoError(t, err)
			},
			allowed: false,
			errMsg:  "",
		},
		{
			desc: "bad response",
			allowedHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`this is not json`))
				require.NoError(t, err)
			},
			allowed: false,
			errMsg:  "decoding response from /allowed endpoint",
		},
		{
			desc: "status multiple choice",
			allowedHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusMultipleChoices)
				_, err := w.Write([]byte(`{"status": true}`))
				require.NoError(t, err)
			},
			allowed: true,
			errMsg:  "",
		},
		{
			desc: "status unauthorized with message",
			allowedHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, err := w.Write([]byte(`{"message": "you're not allowed here'"}`))
				require.NoError(t, err)
			},
			allowed: false,
			errMsg:  "you're not allowed here",
		},
		{
			desc: "status unauthorized",
			allowedHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
			},
			allowed: false,
			errMsg:  "Internal API error",
		},
		{
			desc: "status not found",
			allowedHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				_, err := w.Write([]byte(`{"message": "not found"}`))
				require.NoError(t, err)
			},
			allowed: false,
			errMsg:  "not found",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tc.allowedHandler))
			defer server.Close()

			c, err := NewHTTPClient(config.Gitlab{
				URL:        server.URL,
				SecretFile: secretFilePath,
			}, config.TLS{}, prometheus.Config{})
			require.NoError(t, err)

			mockHistogramVec := promtest.NewMockHistogramVec()
			c.latencyMetric = mockHistogramVec

			allowed, message, err := c.Allowed(context.Background(), AllowedParams{
				RepoPath:                      repo.RelativePath,
				GitObjectDirectory:            repo.GitObjectDirectory,
				GitAlternateObjectDirectories: repo.GitAlternateObjectDirectories,
				GLRepository:                  "repo-1",
				GLID:                          "key-123",
				GLProtocol:                    "http",
				Changes:                       "a\nb\nc\nd",
			})
			require.Equal(t, tc.allowed, allowed)
			if err != nil {
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.Equal(t, tc.errMsg, message)
			}

			require.Equal(t, [][]string{{"allowed"}}, mockHistogramVec.LabelsCalled())
		})
	}
}

func TestAccess_preReceive(t *testing.T) {
	tempDir := testhelper.TempDir(t)

	WriteShellSecretFile(t, tempDir, "secret_token")

	secretFilePath := filepath.Join(tempDir, ".gitlab_shell_secret")

	testCases := []struct {
		desc              string
		prereceiveHandler func(w http.ResponseWriter, r *http.Request)
		success           bool
		errMsg            string
	}{
		{
			desc: "everything ok",
			prereceiveHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`{"reference_counter_increased": true}`))
				require.NoError(t, err)
			},
			success: true,
			errMsg:  "",
		},
		{
			desc: "reference counter not increased",
			prereceiveHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`{"reference_counter_increased": false}`))
				require.NoError(t, err)
			},
			success: false,
			errMsg:  "",
		},
		{
			desc: "server unavailable",
			prereceiveHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, err := w.Write([]byte(`{"message": "server is down!"}`))
				require.NoError(t, err)
			},
			success: false,
			errMsg:  "server is down!",
		},
		{
			desc: "non json content type",
			prereceiveHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`{"reference_counter_increased": true}`))
				require.NoError(t, err)
			},
			success: false,
			errMsg:  "unsupported content type",
		},
		{
			desc: "bad data",
			prereceiveHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`not json`))
				require.NoError(t, err)
			},
			success: false,
			errMsg:  "decoding response from /pre_receive endpoint",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(tc.prereceiveHandler))
			defer server.Close()

			c, err := NewHTTPClient(config.Gitlab{
				URL:        server.URL,
				SecretFile: secretFilePath,
			}, config.TLS{}, prometheus.Config{})
			require.NoError(t, err)

			mockHistogramVec := promtest.NewMockHistogramVec()
			c.latencyMetric = mockHistogramVec

			success, err := c.PreReceive(context.Background(), "key-123")
			require.Equal(t, tc.success, success)
			if err != nil {
				require.Contains(t, err.Error(), tc.errMsg)
			}

			require.Equal(t, [][]string{{"pre-receive"}}, mockHistogramVec.LabelsCalled())
		})
	}
}

func TestAccess_postReceive(t *testing.T) {
	tempDir := testhelper.TempDir(t)

	WriteShellSecretFile(t, tempDir, "secret_token")

	secretFilePath := filepath.Join(tempDir, ".gitlab_shell_secret")
	var receivedRequest postReceiveRequest

	testCases := []struct {
		desc               string
		postReceiveHandler func(w http.ResponseWriter, r *http.Request)
		pushOptions        []string
		success            bool
		errMsg             string
	}{
		{
			desc: "everything ok",
			postReceiveHandler: func(w http.ResponseWriter, r *http.Request) {
				err := json.NewDecoder(r.Body).Decode(&receivedRequest)
				require.NoError(t, err)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, err = w.Write([]byte(`{"reference_counter_decreased": true}`))
				require.NoError(t, err)
			},
			pushOptions: []string{"mr.create", "mr.label=test"},
			success:     true,
			errMsg:      "",
		},
		{
			desc: "reference counter not decreased",
			postReceiveHandler: func(w http.ResponseWriter, r *http.Request) {
				err := json.NewDecoder(r.Body).Decode(&receivedRequest)
				require.NoError(t, err)

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, err = w.Write([]byte(`{"reference_counter_increased": false}`))
				require.NoError(t, err)
			},
			success: false,
			errMsg:  "",
		},
		{
			desc: "server unavailable",
			postReceiveHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, err := w.Write([]byte(`{"message": "server is down!"}`))
				require.NoError(t, err)
			},
			success: false,
			errMsg:  "server is down!",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			receivedRequest = postReceiveRequest{}
			server := httptest.NewServer(http.HandlerFunc(tc.postReceiveHandler))
			defer server.Close()

			c, err := NewHTTPClient(config.Gitlab{
				URL:        server.URL,
				SecretFile: secretFilePath,
			}, config.TLS{}, prometheus.Config{})
			require.NoError(t, err)

			mockHistogramVec := promtest.NewMockHistogramVec()
			c.latencyMetric = mockHistogramVec

			repositoryID := "project-123"
			identifier := "key-123"
			changes := "000 000 refs/heads/master"
			success, _, err := c.PostReceive(context.Background(), repositoryID, identifier, changes, tc.pushOptions...)
			require.Equal(t, tc.success, success)
			if err != nil {
				require.Contains(t, err.Error(), tc.errMsg)
			} else {
				require.Equal(t, repositoryID, receivedRequest.GLRepository)
				require.Equal(t, identifier, receivedRequest.Identifier)
				require.Equal(t, changes, receivedRequest.Changes)
				require.Equal(t, tc.pushOptions, receivedRequest.PushOptions)
			}

			require.Equal(t, [][]string{{"post-receive"}}, mockHistogramVec.LabelsCalled())
		})
	}
}
