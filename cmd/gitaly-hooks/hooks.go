package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/golang/protobuf/jsonpb"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitlabshell"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/stream"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc"
)

func main() {
	var logger = gitalylog.NewHookLogger()

	if len(os.Args) < 2 {
		logger.Fatalf("requires hook name. args: %v", os.Args)
	}

	subCmd := os.Args[1]

	if subCmd == "check" {
		configPath := os.Args[2]

		status, err := check(configPath)
		if err != nil {
			log.Fatal(err)
		}

		os.Exit(status)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if os.Getenv(featureflag.HooksRPCEnvVar) != "true" {
		executeScript(ctx, subCmd, logger)
		return
	}

	repository, err := repositoryFromEnv()
	if err != nil {
		logger.Fatalf("error when getting repository: %v", err)
	}

	gitalySocket, ok := os.LookupEnv("GITALY_SOCKET")
	if !ok {
		logger.Fatal(errors.New("GITALY_SOCKET not set"))
	}

	conn, err := client.Dial("unix://"+gitalySocket, dialOpts(os.Getenv("GITALY_TOKEN")))
	if err != nil {
		logger.Fatalf("error when dialing: %v", err)
	}

	hookClient := gitalypb.NewHookServiceClient(conn)

	hookStatus := int32(1)

	switch subCmd {
	case "update":
		args := os.Args[2:]
		if len(args) != 3 {
			logger.Fatal(errors.New("update hook missing required arguments"))
		}
		ref, oldValue, newValue := args[0], args[1], args[2]

		req := &gitalypb.UpdateHookRequest{
			Repository:           repository,
			EnvironmentVariables: glValues(),
			Ref:                  []byte(ref),
			OldValue:             oldValue,
			NewValue:             newValue,
		}

		updateHookStream, err := hookClient.UpdateHook(ctx, req)
		if err != nil {
			logger.Fatalf("error when starting command for %v: %v", subCmd, err)
		}

		if hookStatus, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
			return updateHookStream.Recv()
		}, noopSender, os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %v: %v", subCmd, err)
		}
	case "pre-receive":
		preReceiveHookStream, err := hookClient.PreReceiveHook(ctx)
		if err != nil {
			logger.Fatalf("error when getting preReceiveHookStream client for %v: %v", subCmd, err)
		}

		if err := preReceiveHookStream.Send(&gitalypb.PreReceiveHookRequest{
			Repository:           repository,
			EnvironmentVariables: glValues(),
		}); err != nil {
			logger.Fatalf("error when sending request for %v: %v", subCmd, err)
		}

		f := sendFunc(streamio.NewWriter(func(p []byte) error {
			return preReceiveHookStream.Send(&gitalypb.PreReceiveHookRequest{Stdin: p})
		}), preReceiveHookStream, os.Stdin)

		if hookStatus, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
			return preReceiveHookStream.Recv()
		}, f, os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %v: %v", subCmd, err)
		}
	case "post-receive":
		postReceiveHookStream, err := hookClient.PostReceiveHook(ctx)
		if err != nil {
			logger.Fatalf("error when getting stream client for %v: %v", subCmd, err)
		}

		if err := postReceiveHookStream.Send(&gitalypb.PostReceiveHookRequest{
			Repository:           repository,
			EnvironmentVariables: glValues(),
			GitPushOptions:       gitPushOptions(),
		}); err != nil {
			logger.Fatalf("error when sending request for %v: %v", subCmd, err)
		}

		f := sendFunc(streamio.NewWriter(func(p []byte) error {
			return postReceiveHookStream.Send(&gitalypb.PostReceiveHookRequest{Stdin: p})
		}), postReceiveHookStream, os.Stdin)

		if hookStatus, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
			return postReceiveHookStream.Recv()
		}, f, os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %v: %v", subCmd, err)
		}
	default:
		logger.Fatal(fmt.Errorf("subcommand name invalid: %v", subCmd))
	}

	os.Exit(int(hookStatus))
}

func noopSender(c chan error) {}

func repositoryFromEnv() (*gitalypb.Repository, error) {
	repoString, ok := os.LookupEnv("GITALY_REPO")
	if !ok {
		return nil, errors.New("GITALY_REPO not found")
	}

	var repo gitalypb.Repository
	if err := jsonpb.UnmarshalString(repoString, &repo); err != nil {
		return nil, err
	}

	pwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	gitObjDirAbs, ok := os.LookupEnv("GIT_OBJECT_DIRECTORY")
	if ok {
		gitObjDir, err := filepath.Rel(pwd, gitObjDirAbs)
		if err != nil {
			return nil, err
		}
		repo.GitObjectDirectory = gitObjDir
	}
	gitAltObjDirsAbs, ok := os.LookupEnv("GIT_ALTERNATE_OBJECT_DIRECTORIES")
	if ok {
		var gitAltObjDirs []string
		for _, gitAltObjDirAbs := range strings.Split(gitAltObjDirsAbs, ":") {
			gitAltObjDir, err := filepath.Rel(pwd, gitAltObjDirAbs)
			if err != nil {
				return nil, err
			}
			gitAltObjDirs = append(gitAltObjDirs, gitAltObjDir)
		}

		repo.GitAlternateObjectDirectories = gitAltObjDirs
	}

	return &repo, nil
}

func glValues() []string {
	var glEnvVars []string
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "GL_") {
			glEnvVars = append(glEnvVars, kv)
		}
	}

	return glEnvVars
}

func gitPushOptions() []string {
	var gitPushOptions []string

	gitPushOptionCount, err := strconv.Atoi(os.Getenv("GIT_PUSH_OPTION_COUNT"))
	if err != nil {
		return gitPushOptions
	}

	for i := 0; i < gitPushOptionCount; i++ {
		gitPushOptions = append(gitPushOptions, os.Getenv(fmt.Sprintf("GIT_PUSH_OPTION_%d", i)))
	}

	return gitPushOptions
}

func dialOpts(token string) []grpc.DialOption {
	dialOpts := client.DefaultDialOpts

	if token != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(token)))
	}

	return dialOpts
}

func sendFunc(reqWriter io.Writer, stream grpc.ClientStream, stdin io.Reader) func(errC chan error) {
	return func(errC chan error) {
		_, errSend := io.Copy(reqWriter, stdin)
		stream.CloseSend()
		errC <- errSend
	}
}

func check(configPath string) (int, error) {
	cfgFile, err := os.Open(configPath)
	if err != nil {
		return 1, fmt.Errorf("error when opening config file: %v", err)
	}
	defer cfgFile.Close()

	var c config.Cfg

	if _, err := toml.DecodeReader(cfgFile, &c); err != nil {
		fmt.Println(err)
		return 1, err
	}

	cmd := exec.Command(filepath.Join(c.GitlabShell.Dir, "bin", "check"))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	gitlabshellEnv, err := gitlabshell.EnvFromConfig(c)
	if err != nil {
		return 1, err
	}
	cmd.Env = append(os.Environ(), gitlabshellEnv...)

	if err = cmd.Run(); err != nil {
		if status, ok := command.ExitStatus(err); ok {
			return status, nil
		}
		return 1, err
	}

	return 0, nil
}

func executeScript(ctx context.Context, subCmd string, logger *gitalylog.HookLogger) {
	gitalyRubyDir := os.Getenv("GITALY_RUBY_DIR")
	if gitalyRubyDir == "" {
		logger.Fatal(errors.New("GITALY_RUBY_DIR not set"))
	}

	rubyHookPath := filepath.Join(gitalyRubyDir, "gitlab-shell", "hooks", subCmd)

	var hookCmd *exec.Cmd

	switch subCmd {
	case "update":
		args := os.Args[2:]
		if len(args) != 3 {
			logger.Fatal(errors.New("update hook missing required arguments"))
		}

		hookCmd = exec.Command(rubyHookPath, args...)
	case "pre-receive", "post-receive":
		hookCmd = exec.Command(rubyHookPath)
	default:
		logger.Fatal(fmt.Errorf("subcommand name invalid: %v", subCmd))
	}

	cmd, err := command.New(ctx, hookCmd, os.Stdin, os.Stdout, os.Stderr, os.Environ()...)
	if err != nil {
		logger.Fatalf("error when starting command for %v: %v", rubyHookPath, err)
	}

	if err := cmd.Wait(); err != nil {
		logger.Errorf("error when executing ruby hook: %v", err)
		exitError, ok := err.(*exec.ExitError)
		if ok {
			os.Exit(exitError.ExitCode())
		}
	}

	os.Exit(0)
}
