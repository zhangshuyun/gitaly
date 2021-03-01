package hook

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestPostReceiveInvalidArgument(t *testing.T) {
	serverSocketPath, stop := runHooksServer(t, config.Config)
	defer stop()

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceiveHook(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.PostReceiveHookRequest{}), "empty repository should result in an error")
	_, err = stream.Recv()

	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestHooksMissingStdin(t *testing.T) {
	user, password, secretToken := "user", "password", "secret token"
	tempDir, cleanup := testhelper.TempDir(t)
	defer cleanup()
	testhelper.WriteShellSecretFile(t, tempDir, secretToken)

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	c := testhelper.GitlabTestServerOptions{
		User:                        user,
		Password:                    password,
		SecretToken:                 secretToken,
		GLID:                        "key_id",
		GLRepository:                testRepo.GetGlRepository(),
		Changes:                     "changes",
		PostReceiveCounterDecreased: true,
		Protocol:                    "protocol",
		RepoPath:                    testRepoPath,
	}

	serverURL, cleanup := testhelper.NewGitlabTestServer(t, c)
	defer cleanup()

	gitlabConfig := config.Gitlab{
		SecretFile: filepath.Join(tempDir, ".gitlab_shell_secret"),
		URL:        serverURL,
		HTTPSettings: config.HTTPSettings{
			User:     user,
			Password: password,
		},
	}

	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)

	config.Config.Gitlab = gitlabConfig

	api, err := gitalyhook.NewGitlabAPI(gitlabConfig, config.Config.TLS)
	require.NoError(t, err)

	testCases := []struct {
		desc    string
		primary bool
		fail    bool
	}{
		{
			desc:    "empty stdin fails if primary",
			primary: true,
			fail:    true,
		},
		{
			desc:    "empty stdin success on secondary",
			primary: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			serverSocketPath, stop := runHooksServerWithAPI(t, api, config.Config)
			defer stop()

			client, conn := newHooksClient(t, serverSocketPath)
			defer conn.Close()

			ctx, cancel := testhelper.Context()
			defer cancel()

			hooksPayload, err := git.NewHooksPayload(
				config.Config,
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
					UserID:   "key_id",
					Username: "username",
					Protocol: "protocol",
				},
				git.PostReceiveHook,
			).Env()
			require.NoError(t, err)

			stream, err := client.PostReceiveHook(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&gitalypb.PostReceiveHookRequest{
				Repository: testRepo,
				EnvironmentVariables: []string{
					hooksPayload,
				},
			}))

			go func() {
				writer := streamio.NewWriter(func(p []byte) error {
					return stream.Send(&gitalypb.PostReceiveHookRequest{Stdin: p})
				})
				_, err := io.Copy(writer, bytes.NewBuffer(nil))
				require.NoError(t, err)
				require.NoError(t, stream.CloseSend(), "close send")
			}()

			var status int32
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}

				status = resp.GetExitStatus().GetValue()
			}

			if tc.fail {
				require.NotEqual(t, int32(0), status, "exit code should be non-zero")
			} else {
				require.Equal(t, int32(0), status, "exit code unequal")
			}
		})
	}
}

func TestPostReceiveMessages(t *testing.T) {
	testCases := []struct {
		desc                         string
		basicMessages, alertMessages []string
		expectedStdout               string
	}{
		{
			desc:          "basic MR message",
			basicMessages: []string{"To create a merge request for okay, visit:\n  http://localhost/project/-/merge_requests/new?merge_request"},
			expectedStdout: `
To create a merge request for okay, visit:
  http://localhost/project/-/merge_requests/new?merge_request
`,
		},
		{
			desc:          "alert",
			alertMessages: []string{"something went very wrong"},
			expectedStdout: `
========================================================================

                       something went very wrong

========================================================================
`,
		},
	}

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	secretToken := "secret token"
	user, password := "user", "password"

	tempDir, cleanup := testhelper.TempDir(t)
	defer cleanup()
	testhelper.WriteShellSecretFile(t, tempDir, secretToken)

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			c := testhelper.GitlabTestServerOptions{
				User:                        user,
				Password:                    password,
				SecretToken:                 secretToken,
				GLID:                        "key_id",
				GLRepository:                testRepo.GetGlRepository(),
				Changes:                     "changes",
				PostReceiveCounterDecreased: true,
				PostReceiveMessages:         tc.basicMessages,
				PostReceiveAlerts:           tc.alertMessages,
				Protocol:                    "protocol",
				RepoPath:                    testRepoPath,
			}

			serverURL, cleanup := testhelper.NewGitlabTestServer(t, c)
			defer cleanup()

			gitlabConfig := config.Gitlab{
				SecretFile: filepath.Join(tempDir, ".gitlab_shell_secret"),
				URL:        serverURL,
				HTTPSettings: config.HTTPSettings{
					User:     user,
					Password: password,
				},
			}

			defer func(cfg config.Cfg) {
				config.Config = cfg
			}(config.Config)

			config.Config.Gitlab = gitlabConfig

			api, err := gitalyhook.NewGitlabAPI(gitlabConfig, config.Config.TLS)
			require.NoError(t, err)

			serverSocketPath, stop := runHooksServerWithAPI(t, api, config.Config)
			defer stop()

			client, conn := newHooksClient(t, serverSocketPath)
			defer conn.Close()

			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.PostReceiveHook(ctx)
			require.NoError(t, err)

			hooksPayload, err := git.NewHooksPayload(config.Config, testRepo, nil, nil, &git.ReceiveHooksPayload{
				UserID:   "key_id",
				Username: "username",
				Protocol: "protocol",
			}, git.PostReceiveHook).Env()
			require.NoError(t, err)

			envVars := []string{
				hooksPayload,
			}

			require.NoError(t, stream.Send(&gitalypb.PostReceiveHookRequest{
				Repository:           testRepo,
				EnvironmentVariables: envVars,
			}))

			go func() {
				writer := streamio.NewWriter(func(p []byte) error {
					return stream.Send(&gitalypb.PostReceiveHookRequest{Stdin: p})
				})
				_, err := writer.Write([]byte("changes"))
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

				_, err = stdout.Write(resp.GetStdout())
				require.NoError(t, err)
				stderr.Write(resp.GetStderr())
				status = resp.GetExitStatus().GetValue()
			}

			assert.Equal(t, int32(0), status)
			assert.Equal(t, "", text.ChompBytes(stderr.Bytes()), "hook stderr")
			assert.Equal(t, tc.expectedStdout, text.ChompBytes(stdout.Bytes()), "hook stdout")
		})
	}
}
