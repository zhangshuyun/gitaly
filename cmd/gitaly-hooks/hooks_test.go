package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/auth"
	internallog "gitlab.com/gitlab-org/gitaly/internal/gitaly/config/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/prometheus"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/gitlab"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/reflection"
)

type glHookValues struct {
	GLID, GLUsername, GLProtocol, GitObjectDir string
	GitAlternateObjectDirs                     []string
}

type proxyValues struct {
	HTTPProxy, HTTPSProxy, NoProxy string
}

var enabledFeatureFlag = featureflag.FeatureFlag{Name: "enabled-feature-flag", OnByDefault: false}
var disabledFeatureFlag = featureflag.FeatureFlag{Name: "disabled-feature-flag", OnByDefault: true}

func rawFeatureFlags() featureflag.Raw {
	ctx := featureflag.IncomingCtxWithFeatureFlag(context.Background(), enabledFeatureFlag)
	ctx = featureflag.IncomingCtxWithDisabledFeatureFlag(ctx, disabledFeatureFlag)
	return featureflag.RawFromContext(ctx)
}

// envForHooks generates a set of environment variables for gitaly hooks
func envForHooks(t testing.TB, cfg config.Cfg, repo *gitalypb.Repository, glHookValues glHookValues, proxyValues proxyValues, gitPushOptions ...string) []string {
	payload, err := git.NewHooksPayload(cfg, repo, nil, nil, &git.ReceiveHooksPayload{
		UserID:   glHookValues.GLID,
		Username: glHookValues.GLUsername,
		Protocol: glHookValues.GLProtocol,
	}, git.AllHooks, rawFeatureFlags()).Env()
	require.NoError(t, err)

	env := append(os.Environ(), []string{
		payload,
		"GITALY_BIN_DIR=" + cfg.BinDir,
		fmt.Sprintf("%s=%s", gitalylog.GitalyLogDirEnvKey, cfg.Logging.Dir),
	}...)
	env = append(env, gitPushOptions...)

	if proxyValues.HTTPProxy != "" {
		env = append(env, fmt.Sprintf("HTTP_PROXY=%s", proxyValues.HTTPProxy))
		env = append(env, fmt.Sprintf("http_proxy=%s", proxyValues.HTTPProxy))
	}
	if proxyValues.HTTPSProxy != "" {
		env = append(env, fmt.Sprintf("HTTPS_PROXY=%s", proxyValues.HTTPSProxy))
		env = append(env, fmt.Sprintf("https_proxy=%s", proxyValues.HTTPSProxy))
	}
	if proxyValues.NoProxy != "" {
		env = append(env, fmt.Sprintf("NO_PROXY=%s", proxyValues.NoProxy))
		env = append(env, fmt.Sprintf("no_proxy=%s", proxyValues.NoProxy))
	}

	if glHookValues.GitObjectDir != "" {
		env = append(env, fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", glHookValues.GitObjectDir))
	}
	if len(glHookValues.GitAlternateObjectDirs) > 0 {
		env = append(env, fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", strings.Join(glHookValues.GitAlternateObjectDirs, ":")))
	}

	return env
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func TestHooksPrePostWithSymlinkedStoragePath(t *testing.T) {
	tempDir := testhelper.TempDir(t)

	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	originalStoragePath := cfg.Storages[0].Path
	symlinkedStoragePath := filepath.Join(tempDir, "storage")
	require.NoError(t, os.Symlink(originalStoragePath, symlinkedStoragePath))
	cfg.Storages[0].Path = symlinkedStoragePath

	testHooksPrePostReceive(t, cfg, repo, repoPath)
}

func TestHooksPrePostReceive(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)
	testHooksPrePostReceive(t, cfg, repo, repoPath)
}

func testHooksPrePostReceive(t *testing.T, cfg config.Cfg, repo *gitalypb.Repository, repoPath string) {
	secretToken := "secret token"
	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"

	changes := "abc"

	gitPushOptions := []string{"gitpushoption1", "gitpushoption2"}
	gitObjectDir := filepath.Join(repoPath, "objects", "temp")
	gitAlternateObjectDirs := []string{filepath.Join(repoPath, "objects")}

	gitlabUser, gitlabPassword := "gitlab_user-1234", "gitlabsecret9887"
	httpProxy, httpsProxy, noProxy := "http://test.example.com:8080", "https://test.example.com:8080", "*"

	c := testhelper.GitlabTestServerOptions{
		User:                        gitlabUser,
		Password:                    gitlabPassword,
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                repo.GetGlRepository(),
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    "ssh",
		GitPushOptions:              gitPushOptions,
		GitObjectDir:                gitObjectDir,
		GitAlternateObjectDirs:      gitAlternateObjectDirs,
		RepoPath:                    repoPath,
	}

	gitlabURL, cleanup := testhelper.NewGitlabTestServer(t, c)
	defer cleanup()
	cfg.Gitlab.URL = gitlabURL
	cfg.Gitlab.SecretFile = testhelper.WriteShellSecretFile(t, cfg.GitlabShell.Dir, secretToken)
	cfg.Gitlab.HTTPSettings.User = gitlabUser
	cfg.Gitlab.HTTPSettings.Password = gitlabPassword

	gitObjectDirRegex := regexp.MustCompile(`(?m)^GIT_OBJECT_DIRECTORY=(.*)$`)
	gitAlternateObjectDirRegex := regexp.MustCompile(`(?m)^GIT_ALTERNATE_OBJECT_DIRECTORIES=(.*)$`)

	hookNames := []string{"pre-receive", "post-receive"}

	for _, hookName := range hookNames {
		t.Run(fmt.Sprintf("hookName: %s", hookName), func(t *testing.T) {
			customHookOutputPath := gittest.WriteEnvToCustomHook(t, repoPath, hookName)

			gitlabClient, err := gitlab.NewHTTPClient(cfg.Gitlab, cfg.TLS, prometheus.Config{})
			require.NoError(t, err)

			stop := runHookServiceWithGitlabClient(t, cfg, gitlabClient)
			defer stop()

			var stderr, stdout bytes.Buffer
			stdin := bytes.NewBuffer([]byte(changes))
			hookPath, err := filepath.Abs(fmt.Sprintf("../../ruby/git-hooks/%s", hookName))
			require.NoError(t, err)
			cmd := exec.Command(hookPath)
			cmd.Stderr = &stderr
			cmd.Stdout = &stdout
			cmd.Stdin = stdin
			cmd.Env = envForHooks(
				t,
				cfg,
				repo,
				glHookValues{
					GLID:                   glID,
					GLUsername:             glUsername,
					GLProtocol:             glProtocol,
					GitObjectDir:           c.GitObjectDir,
					GitAlternateObjectDirs: c.GitAlternateObjectDirs,
				},
				proxyValues{
					HTTPProxy:  httpProxy,
					HTTPSProxy: httpsProxy,
					NoProxy:    noProxy,
				},
				"GIT_PUSH_OPTION_COUNT=2",
				"GIT_PUSH_OPTION_0=gitpushoption1",
				"GIT_PUSH_OPTION_1=gitpushoption2",
			)

			cmd.Dir = repoPath

			require.NoError(t, cmd.Run())
			require.Empty(t, stderr.String())
			require.Empty(t, stdout.String())

			output := string(testhelper.MustReadFile(t, customHookOutputPath))
			requireContainsOnce(t, output, "GL_USERNAME="+glUsername)
			requireContainsOnce(t, output, "GL_ID="+glID)
			requireContainsOnce(t, output, "GL_REPOSITORY="+repo.GetGlRepository())
			requireContainsOnce(t, output, "GL_PROTOCOL="+glProtocol)
			requireContainsOnce(t, output, "GIT_PUSH_OPTION_COUNT=2")
			requireContainsOnce(t, output, "GIT_PUSH_OPTION_0=gitpushoption1")
			requireContainsOnce(t, output, "GIT_PUSH_OPTION_1=gitpushoption2")
			requireContainsOnce(t, output, "HTTP_PROXY="+httpProxy)
			requireContainsOnce(t, output, "http_proxy="+httpProxy)
			requireContainsOnce(t, output, "HTTPS_PROXY="+httpsProxy)
			requireContainsOnce(t, output, "https_proxy="+httpsProxy)
			requireContainsOnce(t, output, "no_proxy="+noProxy)
			requireContainsOnce(t, output, "NO_PROXY="+noProxy)

			if hookName == "pre-receive" {
				gitObjectDirMatches := gitObjectDirRegex.FindStringSubmatch(output)
				require.Len(t, gitObjectDirMatches, 2)
				require.Equal(t, gitObjectDir, gitObjectDirMatches[1])

				gitAlternateObjectDirMatches := gitAlternateObjectDirRegex.FindStringSubmatch(output)
				require.Len(t, gitAlternateObjectDirMatches, 2)
				require.Equal(t, strings.Join(gitAlternateObjectDirs, ":"), gitAlternateObjectDirMatches[1])
			} else {
				require.Contains(t, output, "GL_PROTOCOL="+glProtocol)
			}
		})
	}
}

func TestHooksUpdate(t *testing.T) {
	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"

	customHooksDir := testhelper.TempDir(t)

	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
		Auth:  auth.Config{Token: "abc123"},
		Hooks: config.Hooks{CustomHooksDir: customHooksDir},
	}))
	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	require.NoError(t, os.Symlink(filepath.Join(cfg.GitlabShell.Dir, "config.yml"), filepath.Join(cfg.GitlabShell.Dir, "config.yml")))

	cfg.Gitlab.SecretFile = testhelper.WriteShellSecretFile(t, cfg.GitlabShell.Dir, "the wrong token")

	stop := runHookServiceServer(t, cfg)
	defer stop()

	testHooksUpdate(t, cfg, glHookValues{
		GLID:       glID,
		GLUsername: glUsername,
		GLProtocol: glProtocol,
	})
}

func testHooksUpdate(t *testing.T, cfg config.Cfg, glValues glHookValues) {
	repo, repoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)

	refval, oldval, newval := "refval", strings.Repeat("a", 40), strings.Repeat("b", 40)
	updateHookPath, err := filepath.Abs("../../ruby/git-hooks/update")
	require.NoError(t, err)
	cmd := exec.Command(updateHookPath, refval, oldval, newval)
	cmd.Env = envForHooks(t, cfg, repo, glValues, proxyValues{})
	cmd.Dir = repoPath

	tempDir := testhelper.TempDir(t)

	customHookArgsPath := filepath.Join(tempDir, "containsarguments")
	dumpArgsToTempfileScript := fmt.Sprintf(`#!/usr/bin/env ruby
require 'json'
open('%s', 'w') { |f| f.puts(JSON.dump(ARGV)) }
`, customHookArgsPath)
	// write a custom hook to path/to/repo.git/custom_hooks/update.d/dumpargsscript which dumps the args into a tempfile
	testhelper.WriteExecutable(t, filepath.Join(repoPath, "custom_hooks", "update.d", "dumpargsscript"), []byte(dumpArgsToTempfileScript))

	// write a custom hook to path/to/repo.git/custom_hooks/update which dumps the env into a tempfile
	customHookOutputPath := gittest.WriteEnvToCustomHook(t, repoPath, "update")

	var stdout, stderr bytes.Buffer

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Dir = repoPath

	require.NoError(t, cmd.Run())
	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())

	require.FileExists(t, customHookArgsPath)

	var inputs []string

	b, err := ioutil.ReadFile(customHookArgsPath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &inputs))
	require.Equal(t, []string{refval, oldval, newval}, inputs)

	output := string(testhelper.MustReadFile(t, customHookOutputPath))
	require.Contains(t, output, "GL_USERNAME="+glValues.GLUsername)
	require.Contains(t, output, "GL_ID="+glValues.GLID)
	require.Contains(t, output, "GL_REPOSITORY="+repo.GetGlRepository())
	require.Contains(t, output, "GL_PROTOCOL="+glValues.GLProtocol)
}

func TestHooksPostReceiveFailed(t *testing.T) {
	secretToken := "secret token"
	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"
	changes := "oldhead newhead"

	cfg, repo, repoPath := testcfg.BuildWithRepo(t, testcfg.WithBase(config.Cfg{Auth: auth.Config{Token: "abc123"}}))
	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	// By setting the last parameter to false, the post-receive API call will
	// send back {"reference_counter_increased": false}, indicating something went wrong
	// with the call

	c := testhelper.GitlabTestServerOptions{
		User:                        "",
		Password:                    "",
		SecretToken:                 secretToken,
		Changes:                     changes,
		GLID:                        glID,
		GLRepository:                repo.GetGlRepository(),
		PostReceiveCounterDecreased: false,
		Protocol:                    "ssh",
	}
	serverURL, cleanup := testhelper.NewGitlabTestServer(t, c)
	defer cleanup()
	cfg.Gitlab.URL = serverURL
	cfg.Gitlab.SecretFile = testhelper.WriteShellSecretFile(t, cfg.GitlabShell.Dir, secretToken)

	gitlabClient, err := gitlab.NewHTTPClient(cfg.Gitlab, cfg.TLS, prometheus.Config{})
	require.NoError(t, err)

	customHookOutputPath := gittest.WriteEnvToCustomHook(t, repoPath, "post-receive")

	var stdout, stderr bytes.Buffer

	postReceiveHookPath, err := filepath.Abs("../../ruby/git-hooks/post-receive")
	require.NoError(t, err)

	testcases := []struct {
		desc    string
		primary bool
		verify  func(*testing.T, *exec.Cmd, *bytes.Buffer, *bytes.Buffer)
	}{
		{
			desc:    "Primary calls out to post_receive endpoint",
			primary: true,
			verify: func(t *testing.T, cmd *exec.Cmd, stdout, stderr *bytes.Buffer) {
				err = cmd.Run()
				code, ok := command.ExitStatus(err)
				require.True(t, ok, "expect exit status in %v", err)

				require.Equal(t, 1, code, "exit status")
				require.Empty(t, stdout.String())
				require.Empty(t, stderr.String())
				require.NoFileExists(t, customHookOutputPath)
			},
		},
		{
			desc:    "Secondary does not call out to post_receive endpoint",
			primary: false,
			verify: func(t *testing.T, cmd *exec.Cmd, stdout, stderr *bytes.Buffer) {
				err = cmd.Run()
				require.NoError(t, err)

				require.Empty(t, stdout.String())
				require.Empty(t, stderr.String())
				require.NoFileExists(t, customHookOutputPath)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			stop := runHookServiceWithGitlabClient(t, cfg, gitlabClient)
			defer stop()

			hooksPayload, err := git.NewHooksPayload(
				cfg,
				repo,
				&metadata.Transaction{
					ID:      1,
					Node:    "node",
					Primary: tc.primary,
				},
				&metadata.PraefectServer{
					SocketPath: "/path/to/socket",
					Token:      "secret",
				},
				&git.ReceiveHooksPayload{
					UserID:   glID,
					Username: glUsername,
					Protocol: glProtocol,
				},
				git.PostReceiveHook,
				rawFeatureFlags(),
			).Env()
			require.NoError(t, err)

			env := envForHooks(t, cfg, repo, glHookValues{}, proxyValues{})
			env = append(env, hooksPayload)

			cmd := exec.Command(postReceiveHookPath)
			cmd.Env = env
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			cmd.Stdin = bytes.NewBuffer([]byte(changes))
			cmd.Dir = repoPath

			tc.verify(t, cmd, &stdout, &stderr)
		})
	}
}

func TestHooksNotAllowed(t *testing.T) {
	secretToken := "secret token"
	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"
	changes := "oldhead newhead"

	cfg, repo, repoPath := testcfg.BuildWithRepo(t, testcfg.WithBase(config.Cfg{Auth: auth.Config{Token: "abc123"}}))
	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	c := testhelper.GitlabTestServerOptions{
		User:                        "",
		Password:                    "",
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                repo.GetGlRepository(),
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    "ssh",
	}
	serverURL, cleanup := testhelper.NewGitlabTestServer(t, c)
	defer cleanup()

	cfg.Gitlab.URL = serverURL
	cfg.Gitlab.SecretFile = testhelper.WriteShellSecretFile(t, cfg.GitlabShell.Dir, "the wrong token")

	customHookOutputPath := gittest.WriteEnvToCustomHook(t, repoPath, "post-receive")

	gitlabClient, err := gitlab.NewHTTPClient(cfg.Gitlab, cfg.TLS, prometheus.Config{})
	require.NoError(t, err)

	stop := runHookServiceWithGitlabClient(t, cfg, gitlabClient)
	defer stop()

	var stderr, stdout bytes.Buffer

	preReceiveHookPath, err := filepath.Abs("../../ruby/git-hooks/pre-receive")
	require.NoError(t, err)
	cmd := exec.Command(preReceiveHookPath)
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	cmd.Stdin = strings.NewReader(changes)
	cmd.Env = envForHooks(t, cfg, repo,
		glHookValues{
			GLID:       glID,
			GLUsername: glUsername,
			GLProtocol: glProtocol,
		},
		proxyValues{})
	cmd.Dir = repoPath

	require.Error(t, cmd.Run())
	require.Equal(t, "GitLab: 401 Unauthorized\n", stderr.String())
	require.Equal(t, "", stdout.String())
	require.NoFileExists(t, customHookOutputPath)
}

func TestCheckOK(t *testing.T) {
	user, password := "user123", "password321"

	c := testhelper.GitlabTestServerOptions{
		User:                        user,
		Password:                    password,
		SecretToken:                 "",
		GLRepository:                "",
		Changes:                     "",
		PostReceiveCounterDecreased: false,
		Protocol:                    "ssh",
	}
	serverURL, cleanup := testhelper.NewGitlabTestServer(t, c)
	defer cleanup()

	tempDir := testhelper.TempDir(t)

	gitlabShellDir := filepath.Join(tempDir, "gitlab-shell")
	require.NoError(t, os.MkdirAll(gitlabShellDir, 0755))

	testhelper.WriteShellSecretFile(t, gitlabShellDir, "the secret")
	configPath, cleanup := testhelper.WriteTemporaryGitalyConfigFile(t, tempDir, serverURL, user, password, path.Join(gitlabShellDir, ".gitlab_shell_secret"))
	defer cleanup()

	cfg := testcfg.Build(t)
	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	cmd := exec.Command(filepath.Join(cfg.BinDir, "gitaly-hooks"), "check", configPath)

	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout

	err := cmd.Run()
	require.NoError(t, err)
	require.Empty(t, stderr.String())

	output := stdout.String()
	require.Contains(t, output, "Checking GitLab API access: OK")
	require.Contains(t, output, "Redis reachable for GitLab: true")
}

func TestCheckBadCreds(t *testing.T) {
	user, password := "user123", "password321"

	c := testhelper.GitlabTestServerOptions{
		User:                        user,
		Password:                    password,
		SecretToken:                 "",
		GLRepository:                "",
		Changes:                     "",
		PostReceiveCounterDecreased: false,
		Protocol:                    "ssh",
		GitPushOptions:              nil,
	}
	serverURL, cleanup := testhelper.NewGitlabTestServer(t, c)
	defer cleanup()

	tempDir := testhelper.TempDir(t)

	gitlabShellDir := filepath.Join(tempDir, "gitlab-shell")
	require.NoError(t, os.MkdirAll(gitlabShellDir, 0755))
	testhelper.WriteShellSecretFile(t, gitlabShellDir, "the secret")

	configPath, cleanup := testhelper.WriteTemporaryGitalyConfigFile(t, tempDir, serverURL, "wrong", password, path.Join(gitlabShellDir, ".gitlab_shell_secret"))
	defer cleanup()

	cfg := testcfg.Build(t)
	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	cmd := exec.Command(filepath.Join(cfg.BinDir, "gitaly-hooks"), "check", configPath)

	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout

	require.Error(t, cmd.Run())
	require.Contains(t, stderr.String(), "HTTP GET to GitLab endpoint /check failed: authorization failed")
	require.Regexp(t, `Checking GitLab API access: .* level=error msg="Internal API error" .* error="authorization failed" method=GET status=401 url="http://127.0.0.1:[0-9]+/api/v4/internal/check"\nFAIL`, stdout.String())
}

func runHookServiceServer(t *testing.T, cfg config.Cfg) func() {
	return runHookServiceWithGitlabClient(t, cfg, gitlab.NewMockClient())
}

type featureFlagAsserter struct {
	t       testing.TB
	wrapped gitalypb.HookServiceServer
}

func (svc featureFlagAsserter) assertFlags(ctx context.Context) {
	assert.True(svc.t, featureflag.IsEnabled(ctx, enabledFeatureFlag))
	assert.True(svc.t, featureflag.IsDisabled(ctx, disabledFeatureFlag))
}

func (svc featureFlagAsserter) PreReceiveHook(stream gitalypb.HookService_PreReceiveHookServer) error {
	svc.assertFlags(stream.Context())
	return svc.wrapped.PreReceiveHook(stream)
}

func (svc featureFlagAsserter) PostReceiveHook(stream gitalypb.HookService_PostReceiveHookServer) error {
	svc.assertFlags(stream.Context())
	return svc.wrapped.PostReceiveHook(stream)
}

func (svc featureFlagAsserter) UpdateHook(request *gitalypb.UpdateHookRequest, stream gitalypb.HookService_UpdateHookServer) error {
	svc.assertFlags(stream.Context())
	return svc.wrapped.UpdateHook(request, stream)
}

func (svc featureFlagAsserter) ReferenceTransactionHook(stream gitalypb.HookService_ReferenceTransactionHookServer) error {
	svc.assertFlags(stream.Context())
	return svc.wrapped.ReferenceTransactionHook(stream)
}

func (svc featureFlagAsserter) PackObjectsHook(stream gitalypb.HookService_PackObjectsHookServer) error {
	svc.assertFlags(stream.Context())
	return svc.wrapped.PackObjectsHook(stream)
}

func runHookServiceWithGitlabClient(t *testing.T, cfg config.Cfg, gitlabClient gitlab.Client) func() {
	registry := backchannel.NewRegistry()
	txManager := transaction.NewManager(cfg, registry)
	hookManager := gitalyhook.NewManager(config.NewLocator(cfg), txManager, gitlabClient, cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	server := testhelper.NewServerWithAuth(t, nil, nil, cfg.Auth.Token, registry, testhelper.WithInternalSocket(cfg))

	gitalypb.RegisterHookServiceServer(server.GrpcServer(), featureFlagAsserter{
		t: t, wrapped: hook.NewServer(cfg, hookManager, gitCmdFactory),
	})
	reflection.Register(server.GrpcServer())
	server.Start(t)

	return server.Stop
}

func requireContainsOnce(t *testing.T, s string, contains string) {
	r := regexp.MustCompile(contains)
	matches := r.FindAllStringIndex(s, -1)
	require.Equal(t, 1, len(matches))
}

func TestFixFilterQuoteBug(t *testing.T) {
	testCases := []struct{ in, out string }{
		{"foo bar", "foo bar"},
		{"--filter=blob:none", "--filter=blob:none"},
		{"--filter='blob:none'", "--filter=blob:none"},
		{`--filter='blob'\'':none'`, `--filter=blob':none`},
		{`--filter='blob'\!':none'`, `--filter=blob!:none`},
		{`--filter='blob'\'':none'\!''`, `--filter=blob':none!`},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d-%s", i, tc.in), func(t *testing.T) {
			require.Equal(t, tc.out, fixFilterQuoteBug(tc.in))
		})
	}
}

func TestGitalyHooksPackObjects(t *testing.T) {
	logDir, err := filepath.Abs("testdata")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(logDir, 0755))

	cfg, repo, repoPath := testcfg.BuildWithRepo(t, testcfg.WithBase(config.Cfg{
		Auth:    auth.Config{Token: "abc123"},
		Logging: config.Logging{Config: internallog.Config{Dir: logDir}},
	}))

	testhelper.ConfigureGitalyHooksBin(t, cfg)
	testhelper.ConfigureGitalySSHBin(t, cfg)

	env := envForHooks(t, cfg, repo, glHookValues{}, proxyValues{})

	baseArgs := []string{
		cfg.Git.BinPath,
		"clone",
		"-u",
		"git -c uploadpack.allowFilter -c uploadpack.packObjectsHook=" + cfg.BinDir + "/gitaly-hooks upload-pack",
		"--quiet",
		"--no-local",
		"--bare",
	}

	testCases := []struct {
		desc      string
		extraArgs []string
	}{
		{desc: "regular clone"},
		{desc: "shallow clone", extraArgs: []string{"--depth=1"}},
		{desc: "partial clone", extraArgs: []string{"--filter=blob:none"}},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			defer runHookServiceServer(t, cfg)()

			tempDir := testhelper.TempDir(t)

			args := append(baseArgs[1:], tc.extraArgs...)
			args = append(args, repoPath, tempDir)
			cmd := exec.Command(baseArgs[0], args...)
			cmd.Env = env
			cmd.Stderr = os.Stderr

			require.NoError(t, cmd.Run())
		})
	}
}

func TestRequestedHooks(t *testing.T) {
	for hook, hookName := range map[git.Hook]string{
		git.ReferenceTransactionHook: "reference-transaction",
		git.UpdateHook:               "update",
		git.PreReceiveHook:           "pre-receive",
		git.PostReceiveHook:          "post-receive",
		git.PackObjectsHook:          "git",
	} {
		t.Run(hookName, func(t *testing.T) {
			t.Run("unrequested hook is ignored", func(t *testing.T) {
				cfg := testcfg.Build(t)
				testhelper.ConfigureGitalyHooksBin(t, cfg)
				testhelper.ConfigureGitalySSHBin(t, cfg)

				payload, err := git.NewHooksPayload(cfg, &gitalypb.Repository{}, nil, nil, nil, git.AllHooks&^hook, nil).Env()
				require.NoError(t, err)

				cmd := exec.Command(filepath.Join(cfg.BinDir, "gitaly-hooks"), hookName)
				cmd.Env = []string{payload}
				require.NoError(t, cmd.Run())
			})

			t.Run("requested hook runs", func(t *testing.T) {
				cfg := testcfg.Build(t)
				testhelper.ConfigureGitalyHooksBin(t, cfg)
				testhelper.ConfigureGitalySSHBin(t, cfg)

				payload, err := git.NewHooksPayload(cfg, &gitalypb.Repository{}, nil, nil, nil, hook, nil).Env()
				require.NoError(t, err)

				cmd := exec.Command(filepath.Join(cfg.BinDir, "gitaly-hooks"), hookName)
				cmd.Env = []string{payload}

				// We simply check that there is an error here as an indicator that
				// the hook logic ran. We don't care for the actual error because we
				// know that in the previous testcase without the hook being
				// requested, there was no error.
				require.Error(t, cmd.Run(), "hook should have run and failed due to incomplete setup")
			})
		})
	}
}
