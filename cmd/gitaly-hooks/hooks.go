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
	gitalyauth "gitlab.com/gitlab-org/gitaly/v13/auth"
	"gitlab.com/gitlab-org/gitaly/v13/client"
	"gitlab.com/gitlab-org/gitaly/v13/internal/command"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitlabshell"
	gitalylog "gitlab.com/gitlab-org/gitaly/v13/internal/log"
	"gitlab.com/gitlab-org/gitaly/v13/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/v13/internal/stream"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v13/streamio"
	"gitlab.com/gitlab-org/labkit/tracing"
	"google.golang.org/grpc"
)

// fixupReftxHook fixes up the subcommand in case case the wrong hook was executed instead of the
// reference-transaction hook. This bug was introduced together with the reference-transaction hook
// in Git v2.28.0. As pre-receive and post-receive don't have any arguments and update has the fully
// qualified reference as first argument, none should ever be invoked with the verbs that the
// reference-transaction hook receives.
func fixupReftxHook(args []string) []string {
	if len(args) != 3 {
		return args
	}

	name := args[1]
	if name != "pre-receive" && name != "post-receive" && name != "update" {
		return args
	}

	arg := args[2]
	if arg != "prepared" && arg != "committed" && arg != "aborted" {
		return args
	}

	return append(
		[]string{args[0], "reference-transaction"},
		args[2:]...,
	)
}

func main() {
	var logger = gitalylog.NewHookLogger()

	if len(os.Args) < 2 {
		logger.Fatalf("requires hook name. args: %v", os.Args)
	}

	fixedArgs := fixupReftxHook(os.Args)

	subCmd := fixedArgs[1]

	if subCmd == "check" {
		configPath := fixedArgs[2]

		status, err := check(configPath)
		if err != nil {
			log.Fatal(err)
		}

		os.Exit(status)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Since the environment is sanitized at the moment, we're only
	// using this to extract the correlation ID. The finished() call
	// to clean up the tracing will be a NOP here.
	ctx, finished := tracing.ExtractFromEnv(ctx)
	defer finished()

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
		args := fixedArgs[2:]
		if len(args) != 3 {
			logger.Fatalf("hook %q is missing required arguments", subCmd)
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

		environment := append(glValues(), gitObjectDirs()...)
		for _, key := range []string{metadata.PraefectEnvKey, metadata.TransactionEnvKey} {
			if value, ok := os.LookupEnv(key); ok {
				env := fmt.Sprintf("%s=%s", key, value)
				environment = append(environment, env)
			}
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

		environment := glValues()

		for _, key := range []string{metadata.PraefectEnvKey, metadata.TransactionEnvKey} {
			if value, ok := os.LookupEnv(key); ok {
				env := fmt.Sprintf("%s=%s", key, value)
				environment = append(environment, env)
			}
		}

		if err := postReceiveHookStream.Send(&gitalypb.PostReceiveHookRequest{
			Repository:           repository,
			EnvironmentVariables: environment,
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
	case "reference-transaction":
		if os.Getenv(featureflag.ReferenceTransactionHookEnvVar) != "true" {
			os.Exit(0)
		}

		referenceTransactionHookStream, err := hookClient.ReferenceTransactionHook(ctx)
		if err != nil {
			logger.Fatalf("error when getting referenceTransactionHookStream client for %q: %v", subCmd, err)
		}

		environment := glValues()

		for _, key := range []string{metadata.PraefectEnvKey, metadata.TransactionEnvKey} {
			if value, ok := os.LookupEnv(key); ok {
				env := fmt.Sprintf("%s=%s", key, value)
				environment = append(environment, env)
			}
		}

		if err := referenceTransactionHookStream.Send(&gitalypb.ReferenceTransactionHookRequest{
			Repository:           repository,
			EnvironmentVariables: environment,
		}); err != nil {
			logger.Fatalf("error when sending request for %q: %v", subCmd, err)
		}

		f := sendFunc(streamio.NewWriter(func(p []byte) error {
			return referenceTransactionHookStream.Send(&gitalypb.ReferenceTransactionHookRequest{Stdin: p})
		}), referenceTransactionHookStream, os.Stdin)

		if hookStatus, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
			return referenceTransactionHookStream.Recv()
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

func gitObjectDirs() []string {
	var objectDirs []string
	gitObjectDirectory, ok := os.LookupEnv("GIT_OBJECT_DIRECTORY")
	if ok {
		objectDirs = append(objectDirs, "GIT_OBJECT_DIRECTORY="+gitObjectDirectory)
	}
	gitAlternateObjectDirectories, ok := os.LookupEnv("GIT_ALTERNATE_OBJECT_DIRECTORIES")
	if ok {
		objectDirs = append(objectDirs, "GIT_ALTERNATE_OBJECT_DIRECTORIES="+gitAlternateObjectDirectories)
	}

	return objectDirs
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
