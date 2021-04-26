package hook

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestPreReceiveInvalidArgument(t *testing.T) {
	_, _, _, client := setupHookService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PreReceiveHook(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{}))
	_, err = stream.Recv()

	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func sendPreReceiveHookRequest(t *testing.T, stream gitalypb.HookService_PreReceiveHookClient, stdin io.Reader) ([]byte, []byte, int32, error) {
	go func() {
		writer := streamio.NewWriter(func(p []byte) error {
			return stream.Send(&gitalypb.PreReceiveHookRequest{Stdin: p})
		})
		_, err := io.Copy(writer, stdin)
		require.NoError(t, err)
		require.NoError(t, stream.CloseSend(), "close send")
	}()

	var status int32
	var stdout, stderr bytes.Buffer
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return stdout.Bytes(), stderr.Bytes(), -1, err
		}

		_, err = stdout.Write(resp.GetStdout())
		require.NoError(t, err)
		_, err = stderr.Write(resp.GetStderr())
		require.NoError(t, err)

		status = resp.GetExitStatus().GetValue()
		require.NoError(t, err)
	}

	return stdout.Bytes(), stderr.Bytes(), status, nil
}

func receivePreReceive(t *testing.T, stream gitalypb.HookService_PreReceiveHookClient, stdin io.Reader) ([]byte, []byte, int32) {
	stdout, stderr, status, err := sendPreReceiveHookRequest(t, stream, stdin)
	require.NoError(t, err)
	return stdout, stderr, status
}

func TestPreReceiveHook_GitlabAPIAccess(t *testing.T) {
	user, password := "user", "password"
	secretToken := "secret123"
	glID := "key-123"
	changes := "changes123"
	protocol := "http"

	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	gitObjectDirRel := "git/object/dir"
	gitAlternateObjectRelDirs := []string{"alt/obj/dir/1", "alt/obj/dir/2"}

	gitObjectDirAbs := filepath.Join(repoPath, gitObjectDirRel)
	var gitAlternateObjectAbsDirs []string

	for _, gitAltObjectRel := range gitAlternateObjectRelDirs {
		gitAlternateObjectAbsDirs = append(gitAlternateObjectAbsDirs, filepath.Join(repoPath, gitAltObjectRel))
	}

	tmpDir := testhelper.TempDir(t)
	secretFilePath := filepath.Join(tmpDir, ".gitlab_shell_secret")
	testhelper.WriteShellSecretFile(t, tmpDir, secretToken)

	repo.GitObjectDirectory = gitObjectDirRel
	repo.GitAlternateObjectDirectories = gitAlternateObjectRelDirs

	serverURL, cleanup := testhelper.NewGitlabTestServer(t, testhelper.GitlabTestServerOptions{
		User:                        user,
		Password:                    password,
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                repo.GetGlRepository(),
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    protocol,
		GitPushOptions:              nil,
		GitObjectDir:                gitObjectDirAbs,
		GitAlternateObjectDirs:      gitAlternateObjectAbsDirs,
		RepoPath:                    repoPath,
	})

	defer cleanup()

	gitlabConfig := config.Gitlab{
		URL: serverURL,
		HTTPSettings: config.HTTPSettings{
			User:     user,
			Password: password,
		},
		SecretFile: secretFilePath,
	}

	gitlabAPI, err := gitalyhook.NewGitlabAPI(gitlabConfig, cfg.TLS)
	require.NoError(t, err)

	serverSocketPath := runHooksServer(t, cfg, nil, testserver.WithGitLabAPI(gitlabAPI))

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	hooksPayload, err := git.NewHooksPayload(
		cfg,
		repo,
		nil,
		nil,
		&git.ReceiveHooksPayload{
			UserID:   glID,
			Username: "username",
			Protocol: protocol,
		},
		git.PreReceiveHook,
		featureflag.RawFromContext(ctx),
	).Env()
	require.NoError(t, err)

	stdin := bytes.NewBufferString(changes)
	req := gitalypb.PreReceiveHookRequest{
		Repository: repo,
		EnvironmentVariables: []string{
			hooksPayload,
		},
	}

	stream, err := client.PreReceiveHook(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&req))

	stdout, stderr, status := receivePreReceive(t, stream, stdin)

	require.Equal(t, int32(0), status)
	assert.Equal(t, "", text.ChompBytes(stderr), "hook stderr")
	assert.Equal(t, "", text.ChompBytes(stdout), "hook stdout")
}

func preReceiveHandler(t *testing.T, increased bool) http.HandlerFunc {
	return func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Content-Type", "application/json")
		res.WriteHeader(http.StatusOK)
		_, err := res.Write([]byte(fmt.Sprintf("{\"reference_counter_increased\": %v}", increased)))
		require.NoError(t, err)
	}
}

func allowedHandler(t *testing.T, allowed bool) http.HandlerFunc {
	return func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Content-Type", "application/json")
		if allowed {
			res.WriteHeader(http.StatusOK)
			_, err := res.Write([]byte(`{"status": true}`))
			require.NoError(t, err)
		} else {
			res.WriteHeader(http.StatusUnauthorized)
			_, err := res.Write([]byte(`{"message":"not allowed"}`))
			require.NoError(t, err)
		}
	}
}

func TestPreReceive_APIErrors(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testCases := []struct {
		desc               string
		allowedHandler     http.HandlerFunc
		preReceiveHandler  http.HandlerFunc
		expectedExitStatus int32
		expectedStderr     string
	}{
		{
			desc: "/allowed endpoint returns 401",
			allowedHandler: http.HandlerFunc(
				func(res http.ResponseWriter, req *http.Request) {
					res.Header().Set("Content-Type", "application/json")
					res.WriteHeader(http.StatusUnauthorized)
					_, err := res.Write([]byte(`{"message":"not allowed"}`))
					require.NoError(t, err)
				}),
			expectedExitStatus: 1,
			expectedStderr:     "GitLab: not allowed",
		},
		{
			desc:               "/pre_receive endpoint fails to increase reference coutner",
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, false),
			expectedExitStatus: 1,
		},
	}

	tmpDir := testhelper.TempDir(t)
	secretFilePath := filepath.Join(tmpDir, ".gitlab_shell_secret")
	testhelper.WriteShellSecretFile(t, tmpDir, "token")

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.Handle("/api/v4/internal/allowed", tc.allowedHandler)
			mux.Handle("/api/v4/internal/pre_receive", tc.preReceiveHandler)
			srv := httptest.NewServer(mux)
			defer srv.Close()

			gitlabConfig := config.Gitlab{
				URL:        srv.URL,
				SecretFile: secretFilePath,
			}

			gitlabAPI, err := gitalyhook.NewGitlabAPI(gitlabConfig, cfg.TLS)
			require.NoError(t, err)

			serverSocketPath := runHooksServer(t, cfg, nil, testserver.WithGitLabAPI(gitlabAPI))

			client, conn := newHooksClient(t, serverSocketPath)
			defer conn.Close()

			ctx, cancel := testhelper.Context()
			defer cancel()

			hooksPayload, err := git.NewHooksPayload(
				cfg,
				repo,
				nil,
				nil,
				&git.ReceiveHooksPayload{
					UserID:   "key-123",
					Username: "username",
					Protocol: "web",
				},
				git.PreReceiveHook,
				featureflag.RawFromContext(ctx),
			).Env()
			require.NoError(t, err)

			stream, err := client.PreReceiveHook(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
				Repository: repo,
				EnvironmentVariables: []string{
					hooksPayload,
				},
			}))
			require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
				Stdin: []byte("changes\n"),
			}))
			require.NoError(t, stream.CloseSend())

			_, stderr, status := receivePreReceive(t, stream, &bytes.Buffer{})

			require.Equal(t, tc.expectedExitStatus, status)
			assert.Equal(t, tc.expectedStderr, text.ChompBytes(stderr), "hook stderr")
		})
	}
}

func TestPreReceiveHook_CustomHookErrors(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	mux := http.NewServeMux()
	mux.Handle("/api/v4/internal/allowed", allowedHandler(t, true))
	mux.Handle("/api/v4/internal/pre_receive", preReceiveHandler(t, true))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tmpDir := testhelper.TempDir(t)
	secretFilePath := filepath.Join(tmpDir, ".gitlab_shell_secret")
	testhelper.WriteShellSecretFile(t, tmpDir, "token")

	customHookReturnCode := int32(128)
	customHookReturnMsg := "custom hook error"

	gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(fmt.Sprintf(`#!/bin/bash
echo '%s' 1>&2
exit %d
`, customHookReturnMsg, customHookReturnCode)))

	gitlabConfig := config.Gitlab{
		URL:        srv.URL,
		SecretFile: secretFilePath,
	}

	gitlabAPI, err := gitalyhook.NewGitlabAPI(gitlabConfig, cfg.TLS)
	require.NoError(t, err)

	serverSocketPath := runHooksServer(t, cfg, nil, testserver.WithGitLabAPI(gitlabAPI))

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	hooksPayload, err := git.NewHooksPayload(
		cfg,
		repo,
		nil,
		nil,
		&git.ReceiveHooksPayload{
			UserID:   "key-123",
			Username: "username",
			Protocol: "web",
		},
		git.PreReceiveHook,
		featureflag.RawFromContext(ctx),
	).Env()
	require.NoError(t, err)

	stream, err := client.PreReceiveHook(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
		Repository: repo,
		EnvironmentVariables: []string{
			hooksPayload,
		},
	}))
	require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
		Stdin: []byte("changes\n"),
	}))
	require.NoError(t, stream.CloseSend())

	_, stderr, status := receivePreReceive(t, stream, &bytes.Buffer{})

	require.Equal(t, customHookReturnCode, status)
	assert.Equal(t, customHookReturnMsg, text.ChompBytes(stderr), "hook stderr")
}

func TestPreReceiveHook_Primary(t *testing.T) {
	cfg := testcfg.Build(t)

	cwd, err := os.Getwd()
	require.NoError(t, err)
	cfg.Ruby.Dir = filepath.Join(cwd, "testdata")

	testCases := []struct {
		desc               string
		primary            bool
		allowedHandler     http.HandlerFunc
		preReceiveHandler  http.HandlerFunc
		hookExitCode       int32
		expectedExitStatus int32
		expectedStderr     string
	}{
		{
			desc:               "primary checks for permissions",
			primary:            true,
			allowedHandler:     allowedHandler(t, false),
			expectedExitStatus: 1,
			expectedStderr:     "GitLab: not allowed",
		},
		{
			desc:               "secondary checks for permissions",
			primary:            false,
			allowedHandler:     allowedHandler(t, false),
			expectedExitStatus: 0,
		},
		{
			desc:               "primary tries to increase reference counter",
			primary:            true,
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, false),
			expectedExitStatus: 1,
			expectedStderr:     "",
		},
		{
			desc:               "secondary does not try to increase reference counter",
			primary:            false,
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, false),
			expectedExitStatus: 0,
		},
		{
			desc:               "primary executes hook",
			primary:            true,
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, true),
			hookExitCode:       123,
			expectedExitStatus: 123,
		},
		{
			desc:               "secondary does not execute hook",
			primary:            false,
			allowedHandler:     allowedHandler(t, true),
			preReceiveHandler:  preReceiveHandler(t, true),
			hookExitCode:       123,
			expectedExitStatus: 0,
		},
	}

	for i, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			testRepo, testRepoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg.Storages[0], fmt.Sprintf("repo-%d", i))
			defer cleanupFn()

			mux := http.NewServeMux()
			mux.Handle("/api/v4/internal/allowed", tc.allowedHandler)
			mux.Handle("/api/v4/internal/pre_receive", tc.preReceiveHandler)
			srv := httptest.NewServer(mux)
			defer srv.Close()

			tmpDir := testhelper.TempDir(t)

			secretFilePath := filepath.Join(tmpDir, ".gitlab_shell_secret")
			testhelper.WriteShellSecretFile(t, tmpDir, "token")

			gittest.WriteCustomHook(t, testRepoPath, "pre-receive", []byte(fmt.Sprintf("#!/bin/bash\nexit %d", tc.hookExitCode)))

			gitlabAPI, err := gitalyhook.NewGitlabAPI(config.Gitlab{
				URL:        srv.URL,
				SecretFile: secretFilePath,
			}, cfg.TLS)
			require.NoError(t, err)

			serverSocketPath := runHooksServer(t, cfg, nil, testserver.WithGitLabAPI(gitlabAPI))

			client, conn := newHooksClient(t, serverSocketPath)
			defer conn.Close()

			ctx, cancel := testhelper.Context()
			defer cancel()

			hooksPayload, err := git.NewHooksPayload(
				cfg,
				testRepo,
				&metadata.Transaction{
					ID:      1234,
					Node:    "node-1",
					Primary: tc.primary,
				},
				&metadata.PraefectServer{
					SocketPath: "/path/to/socket",
					Token:      "secret",
				},
				&git.ReceiveHooksPayload{
					UserID:   "key-123",
					Username: "username",
					Protocol: "web",
				},
				git.PreReceiveHook,
				featureflag.RawFromContext(ctx),
			).Env()
			require.NoError(t, err)

			environment := []string{
				hooksPayload,
			}

			stream, err := client.PreReceiveHook(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
				Repository:           testRepo,
				EnvironmentVariables: environment,
			}))
			require.NoError(t, stream.Send(&gitalypb.PreReceiveHookRequest{
				Stdin: []byte("changes\n"),
			}))
			require.NoError(t, stream.CloseSend())

			_, stderr, status, _ := sendPreReceiveHookRequest(t, stream, &bytes.Buffer{})

			require.Equal(t, tc.expectedExitStatus, status)
			require.Equal(t, tc.expectedStderr, text.ChompBytes(stderr))
		})
	}
}
