package main

import (
	"bytes"
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

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
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

// envForHooks generates a set of environment variables for gitaly hooks
func envForHooks(t testing.TB, gitlabShellDir string, repo *gitalypb.Repository, glHookValues glHookValues, proxyValues proxyValues, gitPushOptions ...string) []string {
	payload, err := git.NewHooksPayload(config.Config, repo, nil, nil, &git.ReceiveHooksPayload{
		UserID:   glHookValues.GLID,
		Username: glHookValues.GLUsername,
		Protocol: glHookValues.GLProtocol,
	}).Env()
	require.NoError(t, err)

	env := append(os.Environ(), []string{
		payload,
		"GITALY_BIN_DIR=" + config.Config.BinDir,
		fmt.Sprintf("%s=%s", gitalylog.GitalyLogDirEnvKey, gitlabShellDir),
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

	defer func(rubyDir string) {
		config.Config.Ruby.Dir = rubyDir
	}(config.Config.Ruby.Dir)

	rubyDir, err := filepath.Abs("../../ruby")
	if err != nil {
		log.Error(err)
		return 1
	}

	config.Config.Ruby.Dir = rubyDir

	testhelper.ConfigureGitalyHooksBinary()
	testhelper.ConfigureGitalySSH()

	return m.Run()
}

func TestHooksPrePostWithSymlinkedStoragePath(t *testing.T) {
	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)

	tempDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	originalStoragePath := config.Config.Storages[0].Path
	symlinkedStoragePath := filepath.Join(tempDir, "storage")
	require.NoError(t, os.Symlink(originalStoragePath, symlinkedStoragePath))

	defer func() {
		config.Config.Storages[0].Path = originalStoragePath
	}()
	config.Config.Storages[0].Path = symlinkedStoragePath

	testHooksPrePostReceive(t)
}

func TestHooksPrePostReceive(t *testing.T) {
	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)

	testHooksPrePostReceive(t)
}

func testHooksPrePostReceive(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	secretToken := "secret token"
	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"

	tempGitlabShellDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	changes := "abc"

	gitPushOptions := []string{"gitpushoption1", "gitpushoption2"}
	gitObjectDir := filepath.Join(testRepoPath, "objects", "temp")
	gitAlternateObjectDirs := []string{(filepath.Join(testRepoPath, "objects"))}

	gitlabUser, gitlabPassword := "gitlab_user-1234", "gitlabsecret9887"
	httpProxy, httpsProxy, noProxy := "http://test.example.com:8080", "https://test.example.com:8080", "*"

	c := testhelper.GitlabTestServerOptions{
		User:                        gitlabUser,
		Password:                    gitlabPassword,
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                testRepo.GetGlRepository(),
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    "ssh",
		GitPushOptions:              gitPushOptions,
		GitObjectDir:                gitObjectDir,
		GitAlternateObjectDirs:      gitAlternateObjectDirs,
		RepoPath:                    testRepoPath,
	}

	serverURL, cleanup := testhelper.NewGitlabTestServer(t, c)
	defer cleanup()

	config.Config.GitlabShell.Dir = tempGitlabShellDir

	testhelper.WriteShellSecretFile(t, tempGitlabShellDir, secretToken)

	config.Config.Gitlab.URL = serverURL
	config.Config.Gitlab.SecretFile = filepath.Join(tempGitlabShellDir, ".gitlab_shell_secret")
	config.Config.Gitlab.HTTPSettings.User = gitlabUser
	config.Config.Gitlab.HTTPSettings.Password = gitlabPassword
	config.Config.Auth.Token = "abc123"

	gitObjectDirRegex := regexp.MustCompile(`(?m)^GIT_OBJECT_DIRECTORY=(.*)$`)
	gitAlternateObjectDirRegex := regexp.MustCompile(`(?m)^GIT_ALTERNATE_OBJECT_DIRECTORIES=(.*)$`)

	hookNames := []string{"pre-receive", "post-receive"}

	for _, hookName := range hookNames {
		t.Run(fmt.Sprintf("hookName: %s", hookName), func(t *testing.T) {
			customHookOutputPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			config.Config.Gitlab.URL = serverURL
			config.Config.Gitlab.SecretFile = filepath.Join(tempGitlabShellDir, ".gitlab_shell_secret")
			config.Config.Gitlab.HTTPSettings.User = gitlabUser
			config.Config.Gitlab.HTTPSettings.Password = gitlabPassword

			gitlabAPI, err := gitalyhook.NewGitlabAPI(config.Config.Gitlab, config.Config.TLS)
			require.NoError(t, err)

			stop := runHookServiceServerWithAPI(t, gitlabAPI)
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
				tempGitlabShellDir,
				testRepo,
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

			cmd.Dir = testRepoPath

			require.NoError(t, cmd.Run())
			require.Empty(t, stderr.String())
			require.Empty(t, stdout.String())

			output := string(testhelper.MustReadFile(t, customHookOutputPath))
			requireContainsOnce(t, output, "GL_USERNAME="+glUsername)
			requireContainsOnce(t, output, "GL_ID="+glID)
			requireContainsOnce(t, output, "GL_REPOSITORY="+testRepo.GetGlRepository())
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
	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)

	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"

	tempGitlabShellDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	customHooksDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	os.Symlink(filepath.Join(config.Config.GitlabShell.Dir, "config.yml"), filepath.Join(tempGitlabShellDir, "config.yml"))

	testhelper.WriteShellSecretFile(t, tempGitlabShellDir, "the wrong token")

	config.Config.GitlabShell.Dir = tempGitlabShellDir

	token := "abc123"
	stop := runHookServiceServer(t, token)
	defer stop()

	config.Config.Hooks.CustomHooksDir = customHooksDir

	testHooksUpdate(t, tempGitlabShellDir, token, glHookValues{
		GLID:       glID,
		GLUsername: glUsername,
		GLProtocol: glProtocol,
	})
}

func testHooksUpdate(t *testing.T, gitlabShellDir, token string, glValues glHookValues) {
	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)
	config.Config.Auth.Token = token

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	refval, oldval, newval := "refval", strings.Repeat("a", 40), strings.Repeat("b", 40)
	updateHookPath, err := filepath.Abs("../../ruby/git-hooks/update")
	require.NoError(t, err)
	cmd := exec.Command(updateHookPath, refval, oldval, newval)
	cmd.Env = envForHooks(t, gitlabShellDir, testRepo, glValues, proxyValues{})
	cmd.Dir = testRepoPath

	tempDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	customHookArgsPath := filepath.Join(tempDir, "containsarguments")
	dumpArgsToTempfileScript := fmt.Sprintf(`#!/usr/bin/env ruby
require 'json'
open('%s', 'w') { |f| f.puts(JSON.dump(ARGV)) }
`, customHookArgsPath)
	// write a custom hook to path/to/repo.git/custom_hooks/update.d/dumpargsscript which dumps the args into a tempfile
	cleanup = testhelper.WriteExecutable(t, filepath.Join(testRepoPath, "custom_hooks", "update.d", "dumpargsscript"), []byte(dumpArgsToTempfileScript))
	defer cleanup()

	// write a custom hook to path/to/repo.git/custom_hooks/update which dumps the env into a tempfile
	customHookOutputPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, "update")
	defer cleanup()

	var stdout, stderr bytes.Buffer

	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Dir = testRepoPath

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
	require.Contains(t, output, "GL_REPOSITORY="+testRepo.GetGlRepository())
	require.Contains(t, output, "GL_PROTOCOL="+glValues.GLProtocol)
}

func TestHooksPostReceiveFailed(t *testing.T) {
	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)
	secretToken := "secret token"
	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"
	changes := "oldhead newhead"

	tempGitlabShellDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// By setting the last parameter to false, the post-receive API call will
	// send back {"reference_counter_increased": false}, indicating something went wrong
	// with the call

	c := testhelper.GitlabTestServerOptions{
		User:                        "",
		Password:                    "",
		SecretToken:                 secretToken,
		Changes:                     changes,
		GLID:                        glID,
		GLRepository:                testRepo.GetGlRepository(),
		PostReceiveCounterDecreased: false,
		Protocol:                    "ssh",
	}
	serverURL, cleanup := testhelper.NewGitlabTestServer(t, c)
	defer cleanup()

	testhelper.WriteShellSecretFile(t, tempGitlabShellDir, secretToken)

	config.Config.GitlabShell.Dir = tempGitlabShellDir
	config.Config.Gitlab.URL = serverURL
	config.Config.Gitlab.SecretFile = filepath.Join(tempGitlabShellDir, ".gitlab_shell_secret")
	config.Config.Auth.Token = "abc123"

	gitlabAPI, err := gitalyhook.NewGitlabAPI(config.Config.Gitlab, config.Config.TLS)
	require.NoError(t, err)

	stop := runHookServiceServerWithAPI(t, gitlabAPI)
	defer stop()

	customHookOutputPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, "post-receive")
	defer cleanup()

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

				output := string(testhelper.MustReadFile(t, customHookOutputPath))
				require.Empty(t, output, "custom hook should not have run")
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

				output := string(testhelper.MustReadFile(t, customHookOutputPath))
				require.Empty(t, output, "custom hook should not have run")
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			hooksPayload, err := git.NewHooksPayload(
				config.Config,
				testRepo,
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
			).Env()
			require.NoError(t, err)

			env := envForHooks(t, tempGitlabShellDir, testRepo, glHookValues{}, proxyValues{})
			env = append(env, hooksPayload)

			cmd := exec.Command(postReceiveHookPath)
			cmd.Env = env
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			cmd.Stdin = bytes.NewBuffer([]byte(changes))
			cmd.Dir = testRepoPath

			tc.verify(t, cmd, &stdout, &stderr)
		})
	}
}

func TestHooksNotAllowed(t *testing.T) {
	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)

	secretToken := "secret token"
	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"
	changes := "oldhead newhead"

	tempGitlabShellDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	c := testhelper.GitlabTestServerOptions{
		User:                        "",
		Password:                    "",
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                testRepo.GetGlRepository(),
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    "ssh",
	}
	serverURL, cleanup := testhelper.NewGitlabTestServer(t, c)
	defer cleanup()

	testhelper.WriteShellSecretFile(t, tempGitlabShellDir, "the wrong token")

	config.Config.GitlabShell.Dir = tempGitlabShellDir
	config.Config.Gitlab.URL = serverURL
	config.Config.Gitlab.SecretFile = filepath.Join(tempGitlabShellDir, ".gitlab_shell_secret")
	config.Config.Auth.Token = "abc123"

	customHookOutputPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, "post-receive")
	defer cleanup()

	gitlabAPI, err := gitalyhook.NewGitlabAPI(config.Config.Gitlab, config.Config.TLS)
	require.NoError(t, err)

	stop := runHookServiceServerWithAPI(t, gitlabAPI)
	defer stop()

	var stderr, stdout bytes.Buffer

	preReceiveHookPath, err := filepath.Abs("../../ruby/git-hooks/pre-receive")
	require.NoError(t, err)
	cmd := exec.Command(preReceiveHookPath)
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	cmd.Stdin = strings.NewReader(changes)
	cmd.Env = envForHooks(t, tempGitlabShellDir, testRepo,
		glHookValues{
			GLID:       glID,
			GLUsername: glUsername,
			GLProtocol: glProtocol,
		},
		proxyValues{})
	cmd.Dir = testRepoPath

	require.Error(t, cmd.Run())
	require.Equal(t, "GitLab: 401 Unauthorized\n", stderr.String())
	require.Equal(t, "", stdout.String())

	output := string(testhelper.MustReadFile(t, customHookOutputPath))
	require.Empty(t, output, "custom hook should not have run")
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

	tempDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	gitlabShellDir := filepath.Join(tempDir, "gitlab-shell")
	require.NoError(t, os.MkdirAll(gitlabShellDir, 0755))

	testhelper.WriteShellSecretFile(t, gitlabShellDir, "the secret")
	configPath, cleanup := testhelper.WriteTemporaryGitalyConfigFile(t, tempDir, serverURL, user, password, path.Join(gitlabShellDir, ".gitlab_shell_secret"))
	defer cleanup()

	cmd := exec.Command(filepath.Join(config.Config.BinDir, "gitaly-hooks"), "check", configPath)

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

	tempDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	gitlabShellDir := filepath.Join(tempDir, "gitlab-shell")
	require.NoError(t, os.MkdirAll(gitlabShellDir, 0755))
	testhelper.WriteShellSecretFile(t, gitlabShellDir, "the secret")

	configPath, cleanup := testhelper.WriteTemporaryGitalyConfigFile(t, tempDir, serverURL, "wrong", password, path.Join(gitlabShellDir, ".gitlab_shell_secret"))
	defer cleanup()

	cmd := exec.Command(filepath.Join(config.Config.BinDir, "gitaly-hooks"), "check", configPath)

	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout

	require.Error(t, cmd.Run())
	require.Contains(t, stderr.String(), "HTTP GET to GitLab endpoint /check failed: authorization failed")
	require.Regexp(t, `Checking GitLab API access: .* level=error msg="Internal API error" .* error="authorization failed" method=GET status=401 url="http://127.0.0.1:[0-9]+/api/v4/internal/check"\nFAIL`, stdout.String())
}

func runHookServiceServer(t *testing.T, token string) func() {
	return runHookServiceServerWithAPI(t, gitalyhook.GitlabAPIStub)
}

func runHookServiceServerWithAPI(t *testing.T, gitlabAPI gitalyhook.GitlabAPI) func() {
	server := testhelper.NewServerWithAuth(t, nil, nil, config.Config.Auth.Token, testhelper.WithInternalSocket(config.Config))

	gitalypb.RegisterHookServiceServer(server.GrpcServer(), hook.NewServer(config.Config, gitalyhook.NewManager(config.NewLocator(config.Config), gitlabAPI, config.Config)))
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
	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	logDir, err := filepath.Abs("testdata")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(logDir, 0755))

	env := append(
		envForHooks(t, logDir, testRepo, glHookValues{}, proxyValues{}),
		"GITALY_GIT_BIN_PATH=git",
	)

	baseArgs := []string{
		config.Config.Git.BinPath,
		"clone",
		"-u",
		"git -c uploadpack.allowFilter -c uploadpack.packObjectsHook=" + config.Config.BinDir + "/gitaly-hooks upload-pack",
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
			tempDir, cleanTempDir := testhelper.TempDir(t)
			defer cleanTempDir()

			args := append(baseArgs[1:], tc.extraArgs...)
			args = append(args, testRepoPath, tempDir)
			cmd := exec.Command(baseArgs[0], args...)
			cmd.Env = env
			cmd.Stderr = os.Stderr

			require.NoError(t, cmd.Run())
		})
	}
}
