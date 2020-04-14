package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	hook "gitlab.com/gitlab-org/gitaly/internal/service/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	testhelper.ConfigureGitalyHooksBinary()
	testhelper.ConfigureGitalySSH()

	return m.Run()
}

func TestHooksPrePostReceive(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	secretToken := "secret token"
	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"
	glRepository := "some_repo"

	tempGitlabShellDir, cleanup := testhelper.CreateTemporaryGitlabShellDir(t)
	defer cleanup()

	changes := "abc"

	gitPushOptions := []string{"gitpushoption1", "gitpushoption2"}
	gitObjectDir := filepath.Join(testRepoPath, "objects", "temp")
	gitAlternateObjectDirs := []string{(filepath.Join(testRepoPath, "objects"))}

	c := testhelper.GitlabTestServerOptions{
		User:                        "",
		Password:                    "",
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                glRepository,
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    "ssh",
		GitPushOptions:              gitPushOptions,
		GitObjectDir:                gitObjectDir,
		GitAlternateObjectDirs:      gitAlternateObjectDirs,
		RepoPath:                    testRepoPath,
	}

	ts := testhelper.NewGitlabTestServer(t, c)
	defer ts.Close()
	gitlabShellDir := config.Config.GitlabShell.Dir
	defer func() {
		config.Config.GitlabShell.Dir = gitlabShellDir
	}()

	config.Config.GitlabShell.Dir = tempGitlabShellDir

	testhelper.WriteTemporaryGitlabShellConfigFile(t, tempGitlabShellDir, testhelper.GitlabShellConfig{GitlabURL: ts.URL})
	testhelper.WriteShellSecretFile(t, tempGitlabShellDir, secretToken)

	gitObjectDirRegex := regexp.MustCompile(`(?m)^GIT_OBJECT_DIRECTORY=(.*)$`)
	gitAlternateObjectDirRegex := regexp.MustCompile(`(?m)^GIT_ALTERNATE_OBJECT_DIRECTORIES=(.*)$`)
	srv, socket := runHookServiceServer(t)
	defer srv.Stop()

	testCases := []struct {
		hookName string
		callRPC  bool
	}{
		{
			hookName: "pre-receive",
			callRPC:  false,
		},
		{
			hookName: "post-receive",
			callRPC:  false,
		},
		{
			hookName: "pre-receive",
			callRPC:  true,
		},
		{
			hookName: "post-receive",
			callRPC:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("hookName: %s, calling rpc: %v", tc.hookName, tc.callRPC), func(t *testing.T) {
			customHookOutputPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, tc.hookName)
			defer cleanup()

			var stderr, stdout bytes.Buffer
			stdin := bytes.NewBuffer([]byte(changes))
			hookPath, err := filepath.Abs(fmt.Sprintf("../../ruby/git-hooks/%s", tc.hookName))
			require.NoError(t, err)
			cmd := exec.Command(hookPath)
			cmd.Stderr = &stderr
			cmd.Stdout = &stdout
			cmd.Stdin = stdin
			cmd.Env = testhelper.EnvForHooks(
				t,
				tempGitlabShellDir,
				socket,
				testRepo,
				testhelper.GlHookValues{
					GLID:                   glID,
					GLUsername:             glUsername,
					GLRepo:                 glRepository,
					GLProtocol:             glProtocol,
					GitObjectDir:           c.GitObjectDir,
					GitAlternateObjectDirs: c.GitAlternateObjectDirs,
				},
				gitPushOptions...,
			)

			if tc.callRPC {
				cmd.Env = append(cmd.Env, fmt.Sprintf("%s=true", featureflag.HooksRPCEnvVar))
			}
			cmd.Dir = testRepoPath

			require.NoError(t, cmd.Run())
			require.Empty(t, stderr.String())
			require.Empty(t, stdout.String())

			output := string(testhelper.MustReadFile(t, customHookOutputPath))
			require.Contains(t, output, "GL_USERNAME="+glUsername)
			require.Contains(t, output, "GL_ID="+glID)
			require.Contains(t, output, "GL_REPOSITORY="+glRepository)

			if tc.hookName == "pre-receive" {
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
	glRepository := "some_repo"

	tempGitlabShellDir, cleanup := testhelper.CreateTemporaryGitlabShellDir(t)
	defer cleanup()

	testhelper.WriteTemporaryGitlabShellConfigFile(t, tempGitlabShellDir, testhelper.GitlabShellConfig{GitlabURL: "http://www.example.com"})

	os.Symlink(filepath.Join(config.Config.GitlabShell.Dir, "config.yml"), filepath.Join(tempGitlabShellDir, "config.yml"))

	testhelper.WriteShellSecretFile(t, tempGitlabShellDir, "the wrong token")

	gitlabShellDir := config.Config.GitlabShell.Dir
	defer func() {
		config.Config.GitlabShell.Dir = gitlabShellDir
	}()

	config.Config.GitlabShell.Dir = tempGitlabShellDir

	srv, socket := runHookServiceServer(t)
	defer srv.Stop()

	require.NoError(t, os.MkdirAll(filepath.Join(tempGitlabShellDir, "hooks", "update.d"), 0755))
	testhelper.MustRunCommand(t, nil, "cp", "testdata/update", filepath.Join(tempGitlabShellDir, "hooks", "update.d", "update"))

	for _, callRPC := range []bool{true, false} {
		t.Run(fmt.Sprintf("call rpc: %t", callRPC), func(t *testing.T) {
			testHooksUpdate(t, tempGitlabShellDir, socket, testhelper.GlHookValues{
				GLID:       glID,
				GLUsername: glUsername,
				GLRepo:     glRepository,
				GLProtocol: glProtocol,
			}, callRPC)
		})
	}
}

func testHooksUpdate(t *testing.T, gitlabShellDir, socket string, glValues testhelper.GlHookValues, callRPC bool) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	refval, oldval, newval := "refval", "oldval", "newval"
	updateHookPath, err := filepath.Abs("../../ruby/git-hooks/update")
	require.NoError(t, err)
	cmd := exec.Command(updateHookPath, refval, oldval, newval)
	cmd.Env = testhelper.EnvForHooks(t, gitlabShellDir, socket, testRepo, glValues)
	cmd.Dir = testRepoPath
	tempFilePath := filepath.Join(testRepoPath, "tempfile")

	customHookOutputPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, "update")
	defer cleanup()

	var stdout, stderr bytes.Buffer

	if callRPC {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=true", featureflag.HooksRPCEnvVar))
	}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Dir = testRepoPath

	require.NoError(t, cmd.Run())
	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())

	require.FileExists(t, tempFilePath)

	var inputs []string

	b, err := ioutil.ReadFile(tempFilePath)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &inputs))
	require.Equal(t, []string{refval, oldval, newval}, inputs)

	output := string(testhelper.MustReadFile(t, customHookOutputPath))
	require.Contains(t, output, "GL_USERNAME="+glValues.GLUsername)
	require.Contains(t, output, "GL_ID="+glValues.GLID)
	require.Contains(t, output, "GL_REPOSITORY="+glValues.GLRepo)
	require.Contains(t, output, "GL_PROTOCOL="+glValues.GLProtocol)
}

func TestHooksPostReceiveFailed(t *testing.T) {
	secretToken := "secret token"
	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"
	glRepository := "some_repo"
	changes := "oldhead newhead"

	tempGitlabShellDir, cleanup := testhelper.CreateTemporaryGitlabShellDir(t)
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
		GLRepository:                glRepository,
		PostReceiveCounterDecreased: false,
		Protocol:                    "ssh",
	}
	ts := testhelper.NewGitlabTestServer(t, c)
	defer ts.Close()

	testhelper.WriteTemporaryGitlabShellConfigFile(t, tempGitlabShellDir, testhelper.GitlabShellConfig{GitlabURL: ts.URL})
	testhelper.WriteShellSecretFile(t, tempGitlabShellDir, secretToken)

	gitlabShellDir := config.Config.GitlabShell.Dir
	defer func() {
		config.Config.GitlabShell.Dir = gitlabShellDir
	}()

	config.Config.GitlabShell.Dir = tempGitlabShellDir

	customHookOutputPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, "post-receive")
	defer cleanup()

	srv, socket := runHookServiceServer(t)
	defer srv.Stop()

	var stdout, stderr bytes.Buffer

	postReceiveHookPath, err := filepath.Abs("../../ruby/git-hooks/post-receive")
	require.NoError(t, err)
	cmd := exec.Command(postReceiveHookPath)
	cmd.Env = testhelper.EnvForHooks(t, tempGitlabShellDir, socket, testRepo, testhelper.GlHookValues{
		GLID:       glID,
		GLUsername: glUsername,
		GLRepo:     glRepository,
		GLProtocol: glProtocol,
	})
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Stdin = bytes.NewBuffer([]byte(changes))
	cmd.Dir = testRepoPath

	err = cmd.Run()
	code, ok := command.ExitStatus(err)

	require.True(t, ok, "expect exit status in %v", err)
	require.Equal(t, 1, code, "exit status")
	require.Empty(t, stdout.String())
	require.Empty(t, stderr.String())

	output := string(testhelper.MustReadFile(t, customHookOutputPath))
	require.Empty(t, output, "custom hook should not have run")
}

func TestHooksNotAllowed(t *testing.T) {
	secretToken := "secret token"
	glID := "key-1234"
	glUsername := "iamgitlab"
	glProtocol := "ssh"
	glRepository := "some_repo"
	changes := "oldhead newhead"

	tempGitlabShellDir, cleanup := testhelper.CreateTemporaryGitlabShellDir(t)
	defer cleanup()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	c := testhelper.GitlabTestServerOptions{
		User:                        "",
		Password:                    "",
		SecretToken:                 secretToken,
		GLID:                        glID,
		GLRepository:                glRepository,
		Changes:                     changes,
		PostReceiveCounterDecreased: true,
		Protocol:                    "ssh",
	}
	ts := testhelper.NewGitlabTestServer(t, c)
	defer ts.Close()

	testhelper.WriteTemporaryGitlabShellConfigFile(t, tempGitlabShellDir, testhelper.GitlabShellConfig{GitlabURL: ts.URL})
	testhelper.WriteShellSecretFile(t, tempGitlabShellDir, "the wrong token")

	gitlabShellDir := config.Config.GitlabShell.Dir
	defer func() {
		config.Config.GitlabShell.Dir = gitlabShellDir
	}()

	customHookOutputPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, "post-receive")
	defer cleanup()

	config.Config.GitlabShell.Dir = tempGitlabShellDir
	srv, socket := runHookServiceServer(t)
	defer srv.Stop()

	var stderr, stdout bytes.Buffer

	preReceiveHookPath, err := filepath.Abs("../../ruby/git-hooks/pre-receive")
	require.NoError(t, err)
	cmd := exec.Command(preReceiveHookPath)
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout
	cmd.Stdin = strings.NewReader(changes)
	cmd.Env = testhelper.EnvForHooks(t, tempGitlabShellDir, socket, testRepo, testhelper.GlHookValues{
		GLID:       glID,
		GLUsername: glUsername,
		GLRepo:     glRepository,
		GLProtocol: glProtocol,
	})
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
	ts := testhelper.NewGitlabTestServer(t, c)
	defer ts.Close()

	tempDir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(tempDir)
	}()

	gitlabShellDir := filepath.Join(tempDir, "gitlab-shell")
	binDir := filepath.Join(gitlabShellDir, "bin")
	require.NoError(t, os.MkdirAll(gitlabShellDir, 0755))
	require.NoError(t, os.MkdirAll(binDir, 0755))
	cwd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Symlink(filepath.Join(cwd, "../../ruby/gitlab-shell/bin/check"), filepath.Join(binDir, "check")))

	testhelper.WriteShellSecretFile(t, gitlabShellDir, "the secret")
	testhelper.WriteTemporaryGitlabShellConfigFile(t, gitlabShellDir, testhelper.GitlabShellConfig{GitlabURL: ts.URL, HTTPSettings: testhelper.HTTPSettings{User: user, Password: password}})

	configPath, cleanup := testhelper.WriteTemporaryGitalyConfigFile(t, tempDir, ts.URL, user, password)
	defer cleanup()

	cmd := exec.Command(fmt.Sprintf("%s/gitaly-hooks", config.Config.BinDir), "check", configPath)

	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout

	require.NoError(t, cmd.Run())

	require.Empty(t, stderr.String())
	expectedCheckOutput := "Check GitLab API access: OK\nRedis available via internal API: OK\n"
	require.Equal(t, expectedCheckOutput, stdout.String())
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
	ts := testhelper.NewGitlabTestServer(t, c)
	defer ts.Close()

	tempDir, err := ioutil.TempDir("", t.Name())
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(tempDir)
	}()

	gitlabShellDir := filepath.Join(tempDir, "gitlab-shell")
	binDir := filepath.Join(gitlabShellDir, "bin")
	require.NoError(t, os.MkdirAll(gitlabShellDir, 0755))
	require.NoError(t, os.MkdirAll(binDir, 0755))
	cwd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Symlink(filepath.Join(cwd, "../../ruby/gitlab-shell/bin/check"), filepath.Join(binDir, "check")))

	testhelper.WriteTemporaryGitlabShellConfigFile(t, gitlabShellDir, testhelper.GitlabShellConfig{GitlabURL: ts.URL, HTTPSettings: testhelper.HTTPSettings{User: user + "wrong", Password: password}})
	testhelper.WriteShellSecretFile(t, gitlabShellDir, "the secret")

	configPath, cleanup := testhelper.WriteTemporaryGitalyConfigFile(t, tempDir, ts.URL, user+"wrong", password)
	defer cleanup()

	cmd := exec.Command(fmt.Sprintf("%s/gitaly-hooks", config.Config.BinDir), "check", configPath)

	var stderr, stdout bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout

	require.Error(t, cmd.Run())
	require.Equal(t, "Check GitLab API access: ", stdout.String())
	require.Equal(t, "FAILED. code: 401\n", stderr.String())
}

func runHookServiceServer(t *testing.T) (*grpc.Server, string) {
	server := testhelper.NewTestGrpcServer(t, nil, nil)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterHookServiceServer(server, hook.NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server, serverSocketPath
}
