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
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
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

	repository, err := repositoryFromEnv()
	if err != nil {
		logger.Fatalf("error when getting repository: %v", err)
	}

	conn, err := gitalyFromEnv()
	if err != nil {
		logger.Fatalf("error when connecting to gitaly: %v", err)
	}

	hookClient := gitalypb.NewHookServiceClient(conn)

	hookStatus := int32(1)

	switch subCmd {
	case "update":
		args := os.Args[2:]
		if len(args) != 3 {
			logger.Fatalf("hook %q is missing required arguments", subCmd)
		}
		ref, oldValue, newValue := args[0], args[1], args[2]

		environment := glValues()
		if os.Getenv(featureflag.GoUpdateHookEnvVar) == "true" {
			environment = append(environment, fmt.Sprintf("%s=true", featureflag.GoUpdateHookEnvVar))
		}

		req := &gitalypb.UpdateHookRequest{
			Repository:           repository,
			EnvironmentVariables: environment,
			Ref:                  []byte(ref),
			OldValue:             oldValue,
			NewValue:             newValue,
		}

		updateHookStream, err := hookClient.UpdateHook(ctx, req)
		if err != nil {
			logger.Fatalf("error when starting command for %q: %v", subCmd, err)
		}

		if hookStatus, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
			return updateHookStream.Recv()
		}, noopSender, os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %q: %v", subCmd, err)
		}
	case "pre-receive":
		preReceiveHookStream, err := hookClient.PreReceiveHook(ctx)
		if err != nil {
			logger.Fatalf("error when getting preReceiveHookStream client for %q: %v", subCmd, err)
		}

		environment := glValues()

		for _, key := range []string{metadata.PraefectEnvKey, metadata.TransactionEnvKey} {
			if value, ok := os.LookupEnv(key); ok {
				env := fmt.Sprintf("%s=%s", key, value)
				environment = append(environment, env)
			}
		}

		if os.Getenv(featureflag.GoPreReceiveHookEnvVar) == "true" {
			environment = append(environment, fmt.Sprintf("%s=true", featureflag.GoPreReceiveHookEnvVar))
		}

		if err := preReceiveHookStream.Send(&gitalypb.PreReceiveHookRequest{
			Repository:           repository,
			EnvironmentVariables: environment,
			GitPushOptions:       gitPushOptions(),
		}); err != nil {
			logger.Fatalf("error when sending request for %q: %v", subCmd, err)
		}

		f := sendFunc(streamio.NewWriter(func(p []byte) error {
			return preReceiveHookStream.Send(&gitalypb.PreReceiveHookRequest{Stdin: p})
		}), preReceiveHookStream, os.Stdin)

		if hookStatus, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
			return preReceiveHookStream.Recv()
		}, f, os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %q: %v", subCmd, err)
		}
	case "post-receive":
		postReceiveHookStream, err := hookClient.PostReceiveHook(ctx)
		if err != nil {
			logger.Fatalf("error when getting stream client for %q: %v", subCmd, err)
		}

		if err := postReceiveHookStream.Send(&gitalypb.PostReceiveHookRequest{
			Repository:           repository,
			EnvironmentVariables: glValues(),
			GitPushOptions:       gitPushOptions(),
		}); err != nil {
			logger.Fatalf("error when sending request for %q: %v", subCmd, err)
		}

		f := sendFunc(streamio.NewWriter(func(p []byte) error {
			return postReceiveHookStream.Send(&gitalypb.PostReceiveHookRequest{Stdin: p})
		}), postReceiveHookStream, os.Stdin)

		if hookStatus, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
			return postReceiveHookStream.Recv()
		}, f, os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %q: %v", subCmd, err)
		}
	default:
		logger.Fatalf("subcommand name invalid: %q", subCmd)
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
		return nil, fmt.Errorf("unmarshal JSON %q: %w", repoString, err)
	}

	pwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("can't define working directory: %w", err)
	}

	gitObjDirAbs, ok := os.LookupEnv("GIT_OBJECT_DIRECTORY")
	if ok {
		gitObjDir, err := filepath.Rel(pwd, gitObjDirAbs)
		if err != nil {
			return nil, fmt.Errorf("can't define rel path %q: %w", gitObjDirAbs, err)
		}
		repo.GitObjectDirectory = gitObjDir
	}
	gitAltObjDirsAbs, ok := os.LookupEnv("GIT_ALTERNATE_OBJECT_DIRECTORIES")
	if ok {
		var gitAltObjDirs []string
		for _, gitAltObjDirAbs := range strings.Split(gitAltObjDirsAbs, ":") {
			gitAltObjDir, err := filepath.Rel(pwd, gitAltObjDirAbs)
			if err != nil {
				return nil, fmt.Errorf("can't define rel path %q: %w", gitAltObjDirAbs, err)
			}
			gitAltObjDirs = append(gitAltObjDirs, gitAltObjDir)
		}

		repo.GitAlternateObjectDirectories = gitAltObjDirs
	}

	return &repo, nil
}

func gitalyFromEnv() (*grpc.ClientConn, error) {
	gitalySocket := os.Getenv("GITALY_SOCKET")
	if gitalySocket == "" {
		return nil, errors.New("GITALY_SOCKET not set")
	}

	gitalyToken, ok := os.LookupEnv("GITALY_TOKEN")
	if !ok {
		return nil, errors.New("GITALY_TOKEN not set")
	}

	dialOpts := client.DefaultDialOpts
	if gitalyToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(gitalyToken)))
	}

	conn, err := client.Dial("unix://"+gitalySocket, dialOpts)
	if err != nil {
		return nil, fmt.Errorf("error when dialing: %w", err)
	}

	return conn, nil
}

func glValues() []string {
	glEnvVars := command.AllowedEnvironment()

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
		return 1, fmt.Errorf("failed to open config file: %w", err)
	}
	defer cfgFile.Close()

	var c config.Cfg

	if _, err := toml.DecodeReader(cfgFile, &c); err != nil {
		return 1, fmt.Errorf("failed to decode toml: %w", err)
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
		return 1, fmt.Errorf("failed to run %q: %w", cmd.String(), err)
	}

	return 0, nil
}
