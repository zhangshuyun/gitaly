package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitlab"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/stream"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"gitlab.com/gitlab-org/labkit/tracing"
	"google.golang.org/grpc"
)

type hookCommand struct {
	exec     func(context.Context, git.HooksPayload, gitalypb.HookServiceClient, []string) (int, error)
	hookType git.Hook
}

var (
	hooksBySubcommand = map[string]hookCommand{
		"update": hookCommand{
			exec:     updateHook,
			hookType: git.UpdateHook,
		},
		"pre-receive": hookCommand{
			exec:     preReceiveHook,
			hookType: git.PreReceiveHook,
		},
		"post-receive": hookCommand{
			exec:     postReceiveHook,
			hookType: git.PostReceiveHook,
		},
		"reference-transaction": hookCommand{
			exec:     referenceTransactionHook,
			hookType: git.ReferenceTransactionHook,
		},
		"git": hookCommand{
			exec:     packObjectsHook,
			hookType: git.PackObjectsHook,
		},
	}

	logger *gitalylog.HookLogger
)

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

	hookCommand, ok := hooksBySubcommand[subCmd]
	if !ok {
		logger.Fatalf("subcommand name invalid: %q", subCmd)
	}

	// If the hook wasn't requested, then we simply skip executing any
	// logic.
	if !payload.IsHookRequested(hookCommand.hookType) {
		os.Exit(0)
	}

	conn, err := dialGitaly(payload)
	if err != nil {
		logger.Fatalf("error when connecting to gitaly: %v", err)
	}
	hookClient := gitalypb.NewHookServiceClient(conn)

	ctx = featureflag.OutgoingWithRaw(ctx, payload.FeatureFlags)
	returnCode, err := hookCommand.exec(ctx, payload, hookClient, os.Args)
	if err != nil {
		logger.Fatal(err)
	}

	os.Exit(returnCode)
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

func check(configPath string) (*gitlab.CheckInfo, error) {
	cfgFile, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer cfgFile.Close()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return nil, err
	}

	gitlabAPI, err := gitlab.NewHTTPClient(cfg.Gitlab, cfg.TLS, prometheus.Config{})
	if err != nil {
		return nil, err
	}

	return hook.NewManager(config.NewLocator(cfg), nil, gitlabAPI, cfg).Check(context.TODO())
}

func updateHook(ctx context.Context, payload git.HooksPayload, hookClient gitalypb.HookServiceClient, args []string) (int, error) {
	args = args[2:]
	if len(args) != 3 {
		return 1, errors.New("update hook expects exactly three arguments")
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
		return 1, fmt.Errorf("error when starting command for update hook: %v", err)
	}

	var returnCode int32
	if returnCode, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return updateHookStream.Recv()
	}, noopSender, os.Stdout, os.Stderr); err != nil {
		return 1, fmt.Errorf("error when receiving data for update hook: %v", err)
	}

	return int(returnCode), nil
}

func preReceiveHook(ctx context.Context, payload git.HooksPayload, hookClient gitalypb.HookServiceClient, args []string) (int, error) {
	preReceiveHookStream, err := hookClient.PreReceiveHook(ctx)
	if err != nil {
		return 1, fmt.Errorf("error when getting preReceiveHookStream client for: %v", err)
	}

	if err := preReceiveHookStream.Send(&gitalypb.PreReceiveHookRequest{
		Repository:           payload.Repo,
		EnvironmentVariables: os.Environ(),
		GitPushOptions:       gitPushOptions(),
	}); err != nil {
		return 1, fmt.Errorf("error when sending request for pre-receive hook: %v", err)
	}

	f := sendFunc(streamio.NewWriter(func(p []byte) error {
		return preReceiveHookStream.Send(&gitalypb.PreReceiveHookRequest{Stdin: p})
	}), preReceiveHookStream, os.Stdin)

	var returnCode int32
	if returnCode, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return preReceiveHookStream.Recv()
	}, f, os.Stdout, os.Stderr); err != nil {
		return 1, fmt.Errorf("error when receiving data for pre-receive hook: %v", err)
	}

	return int(returnCode), nil
}

func postReceiveHook(ctx context.Context, payload git.HooksPayload, hookClient gitalypb.HookServiceClient, args []string) (int, error) {
	postReceiveHookStream, err := hookClient.PostReceiveHook(ctx)
	if err != nil {
		return 1, fmt.Errorf("error when getting stream client for post-receive hook: %v", err)
	}

	if err := postReceiveHookStream.Send(&gitalypb.PostReceiveHookRequest{
		Repository:           payload.Repo,
		EnvironmentVariables: os.Environ(),
		GitPushOptions:       gitPushOptions(),
	}); err != nil {
		return 1, fmt.Errorf("error when sending request for post-receive hook: %v", err)
	}

	f := sendFunc(streamio.NewWriter(func(p []byte) error {
		return postReceiveHookStream.Send(&gitalypb.PostReceiveHookRequest{Stdin: p})
	}), postReceiveHookStream, os.Stdin)

	var returnCode int32
	if returnCode, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return postReceiveHookStream.Recv()
	}, f, os.Stdout, os.Stderr); err != nil {
		return 1, fmt.Errorf("error when receiving data for post-receive hook: %v", err)
	}

	return int(returnCode), nil
}

func referenceTransactionHook(ctx context.Context, payload git.HooksPayload, hookClient gitalypb.HookServiceClient, args []string) (int, error) {
	if len(args) != 3 {
		return 1, errors.New("reference-transaction hook is missing required arguments")
	}

	var state gitalypb.ReferenceTransactionHookRequest_State
	switch args[2] {
	case "prepared":
		state = gitalypb.ReferenceTransactionHookRequest_PREPARED
	case "committed":
		state = gitalypb.ReferenceTransactionHookRequest_COMMITTED
	case "aborted":
		state = gitalypb.ReferenceTransactionHookRequest_ABORTED
	default:
		return 1, fmt.Errorf("reference-transaction hook has invalid state: %q", args[2])
	}

	referenceTransactionHookStream, err := hookClient.ReferenceTransactionHook(ctx)
	if err != nil {
		return 1, fmt.Errorf("error when getting referenceTransactionHookStream client: %v", err)
	}

	if err := referenceTransactionHookStream.Send(&gitalypb.ReferenceTransactionHookRequest{
		Repository:           payload.Repo,
		EnvironmentVariables: os.Environ(),
		State:                state,
	}); err != nil {
		return 1, fmt.Errorf("error when sending request for reference-transaction hook: %v", err)
	}

	f := sendFunc(streamio.NewWriter(func(p []byte) error {
		return referenceTransactionHookStream.Send(&gitalypb.ReferenceTransactionHookRequest{Stdin: p})
	}), referenceTransactionHookStream, os.Stdin)

	var returnCode int32
	if returnCode, err = stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return referenceTransactionHookStream.Recv()
	}, f, os.Stdout, os.Stderr); err != nil {
		return 1, fmt.Errorf("error when receiving data for reference-transaction hook: %v", err)
	}

	return int(returnCode), nil
}

func packObjectsHook(ctx context.Context, payload git.HooksPayload, hookClient gitalypb.HookServiceClient, args []string) (int, error) {
	var fixedArgs []string
	for _, a := range args[2:] {
		fixedArgs = append(fixedArgs, fixFilterQuoteBug(a))
	}

	if err := handlePackObjects(ctx, hookClient, payload.Repo, fixedArgs); err != nil {
		logger.Logger().WithFields(logrus.Fields{"args": args}).WithError(err).Error("PackObjectsHook RPC failed")
		return 1, nil
	}

	return 0, nil
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

func handlePackObjects(ctx context.Context, hookClient gitalypb.HookServiceClient, repo *gitalypb.Repository, args []string) error {
	packObjectsStream, err := hookClient.PackObjectsHook(ctx)
	if err != nil {
		return fmt.Errorf("initiate rpc: %w", err)
	}

	if err := packObjectsStream.Send(&gitalypb.PackObjectsHookRequest{
		Repository: repo,
		Args:       args,
	}); err != nil {
		return fmt.Errorf("first request: %w", err)
	}

	stdin := sendFunc(streamio.NewWriter(func(p []byte) error {
		return packObjectsStream.Send(&gitalypb.PackObjectsHookRequest{Stdin: p})
	}), packObjectsStream, os.Stdin)

	if _, err := stream.Handler(func() (stream.StdoutStderrResponse, error) {
		resp, err := packObjectsStream.Recv()
		return nopExitStatus{resp}, err
	}, stdin, os.Stdout, os.Stderr); err != nil {
		return fmt.Errorf("handle stream: %w", err)
	}

	return nil
}

type stdoutStderr interface {
	GetStdout() []byte
	GetStderr() []byte
}

type nopExitStatus struct {
	stdoutStderr
}

func (nopExitStatus) GetExitStatus() *gitalypb.ExitStatus { return nil }
