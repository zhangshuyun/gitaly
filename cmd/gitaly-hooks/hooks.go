package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/stream"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"gitlab.com/gitlab-org/labkit/tracing"
	"google.golang.org/grpc"
)

var logger *gitalylog.HookLogger

func main() {
	logger = gitalylog.NewHookLogger()

	if len(os.Args) < 2 {
		logger.Fatalf("requires hook name. args: %v", os.Args)
	}

	subCmd := os.Args[1]

	if subCmd == "check" {
		logrus.SetLevel(logrus.ErrorLevel)
		if len(os.Args) != 3 {
			log.Fatal(errors.New("no configuration file path provided invoke with: gitaly-hooks check <config_path>"))
		}

		configPath := os.Args[2]
		fmt.Print("Checking GitLab API access: ")

		info, err := check(configPath)
		if err != nil {
			fmt.Print("FAIL\n")
			log.Fatal(err)
		}

		fmt.Print("OK\n")
		fmt.Printf("GitLab version: %s\n", info.Version)
		fmt.Printf("GitLab revision: %s\n", info.Revision)
		fmt.Printf("GitLab Api version: %s\n", info.APIVersion)
		fmt.Printf("Redis reachable for GitLab: %t\n", info.RedisReachable)
		fmt.Println("OK")
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Since the environment is sanitized at the moment, we're only
	// using this to extract the correlation ID. The finished() call
	// to clean up the tracing will be a NOP here.
	ctx, finished := tracing.ExtractFromEnv(ctx)
	defer finished()

	payload, err := git.HooksPayloadFromEnv(os.Environ())
	if err != nil {
		logger.Fatalf("error when getting hooks payload: %v", err)
	}

	conn, err := dialGitaly(payload)
	if err != nil {
		logger.Fatalf("error when connecting to gitaly: %v", err)
	}

	hookClient := gitalypb.NewHookServiceClient(conn)

	hookStatus := int32(1)

	switch subCmd {
	case "update":
		args := os.Args[2:]
		if len(args) != 3 {
			logger.Fatalf("hook %q expects exactly three arguments", subCmd)
		}
		ref, oldValue, newValue := args[0], args[1], args[2]

		req := &gitalypb.UpdateHookRequest{
			Repository:           payload.Repo,
			EnvironmentVariables: os.Environ(),
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

		if err := preReceiveHookStream.Send(&gitalypb.PreReceiveHookRequest{
			Repository:           payload.Repo,
			EnvironmentVariables: os.Environ(),
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
			Repository:           payload.Repo,
			EnvironmentVariables: os.Environ(),
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
		if len(os.Args) != 3 {
			logger.Fatalf("hook %q is missing required arguments", subCmd)
		}

		var state gitalypb.ReferenceTransactionHookRequest_State
		switch os.Args[2] {
		case "prepared":
			state = gitalypb.ReferenceTransactionHookRequest_PREPARED
		case "committed":
			state = gitalypb.ReferenceTransactionHookRequest_COMMITTED
		case "aborted":
			state = gitalypb.ReferenceTransactionHookRequest_ABORTED
		default:
			logger.Fatalf("hook %q has invalid state %s", subCmd, os.Args[2])
		}

		referenceTransactionHookStream, err := hookClient.ReferenceTransactionHook(ctx)
		if err != nil {
			logger.Fatalf("error when getting referenceTransactionHookStream client for %q: %v", subCmd, err)
		}

		if err := referenceTransactionHookStream.Send(&gitalypb.ReferenceTransactionHookRequest{
			Repository:           payload.Repo,
			EnvironmentVariables: os.Environ(),
			State:                state,
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
	case "git":
		var args []string
		for _, a := range os.Args[2:] {
			args = append(args, fixFilterQuoteBug(a))
		}

		hookStatus = 0
		if fallBackGit(args) != nil {
			hookStatus = 1
		}
	default:
		logger.Fatalf("subcommand name invalid: %q", subCmd)
	}

	os.Exit(int(hookStatus))
}

func noopSender(c chan error) {}

func dialGitaly(payload git.HooksPayload) (*grpc.ClientConn, error) {
	dialOpts := client.DefaultDialOpts
	if payload.InternalSocketToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(payload.InternalSocketToken)))
	}

	conn, err := client.Dial("unix://"+payload.InternalSocket, dialOpts)
	if err != nil {
		return nil, fmt.Errorf("error when dialing: %w", err)
	}

	return conn, nil
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

func check(configPath string) (*hook.CheckInfo, error) {
	cfgFile, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer cfgFile.Close()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return nil, err
	}

	gitlabAPI, err := hook.NewGitlabAPI(cfg.Gitlab, cfg.TLS)
	if err != nil {
		return nil, err
	}

	return hook.NewManager(config.NewLocator(cfg), gitlabAPI, cfg).Check(context.TODO())
}

// This is a workaround for a bug in Git:
// https://gitlab.com/gitlab-org/git/-/issues/82. Once that bug is fixed
// we should no longer need this. The fix function is harmless if the bug
// is not present.
func fixFilterQuoteBug(arg string) string {
	const prefix = "--filter='"

	if !(strings.HasPrefix(arg, prefix) && strings.HasSuffix(arg, "'")) {
		return arg
	}

	filterSpec := arg[len(prefix) : len(arg)-1]

	// Perform the inverse of sq_quote_buf() in quote.c. The surrounding quotes
	// are already gone, we now need to undo escaping of ! and '. The escape
	// patterns are '\!' and '\'' respectively.
	filterSpec = strings.ReplaceAll(filterSpec, `'\!'`, `!`)
	filterSpec = strings.ReplaceAll(filterSpec, `'\''`, `'`)

	return "--filter=" + filterSpec
}

func fallBackGit(args []string) error {
	gitCmd := exec.Command(os.Getenv("GITALY_GIT_BIN_PATH"), args...)
	gitCmd.Stdin = os.Stdin
	gitCmd.Stdout = os.Stdout
	gitCmd.Stderr = os.Stderr

	entry := logger.Logger().WithFields(logrus.Fields{
		"args": args,
	})
	const message = "local git command"

	err := gitCmd.Run()
	if err != nil {
		entry.WithError(err).Error(message)
	} else {
		entry.Info(message)
	}

	return err
}
