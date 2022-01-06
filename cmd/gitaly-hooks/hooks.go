package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/env"
	gitalylog "gitlab.com/gitlab-org/gitaly/v14/internal/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/stream"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"gitlab.com/gitlab-org/labkit/tracing"
	"google.golang.org/grpc"
)

type hookError struct {
	returnCode int
	err        error
}

func (e hookError) Error() string {
	return fmt.Sprintf("hook returned error code %d", e.returnCode)
}

type hookCommand struct {
	exec     func(context.Context, git.HooksPayload, gitalypb.HookServiceClient, []string) error
	hookType git.Hook
}

var hooksBySubcommand = map[string]hookCommand{
	"update": {
		exec:     updateHook,
		hookType: git.UpdateHook,
	},
	"pre-receive": {
		exec:     preReceiveHook,
		hookType: git.PreReceiveHook,
	},
	"post-receive": {
		exec:     postReceiveHook,
		hookType: git.PostReceiveHook,
	},
	"reference-transaction": {
		exec:     referenceTransactionHook,
		hookType: git.ReferenceTransactionHook,
	},
	"git": {
		exec:     packObjectsHook,
		hookType: git.PackObjectsHook,
	},
}

func main() {
	logger := gitalylog.NewHookLogger()

	if err := run(os.Args); err != nil {
		var hookError hookError
		if errors.As(err, &hookError) {
			if hookError.err != nil {
				logger.Fatalf("%s", err)
			}
			os.Exit(hookError.returnCode)
		}

		logger.Fatalf("%s", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("requires hook name. args: %v", args)
	}

	subCmd := args[1]

	if subCmd == "check" {
		logrus.SetLevel(logrus.ErrorLevel)
		if len(args) != 3 {
			fmt.Fprint(os.Stderr, "no configuration file path provided invoke with: gitaly-hooks check <config_path>")
			os.Exit(1)
		}

		configPath := args[2]
		fmt.Print("Checking GitLab API access: ")

		info, err := check(configPath)
		if err != nil {
			fmt.Print("FAIL\n")
			fmt.Fprint(os.Stderr, err)
			os.Exit(1)
		}

		fmt.Print("OK\n")
		fmt.Printf("GitLab version: %s\n", info.Version)
		fmt.Printf("GitLab revision: %s\n", info.Revision)
		fmt.Printf("GitLab Api version: %s\n", info.APIVersion)
		fmt.Printf("Redis reachable for GitLab: %t\n", info.RedisReachable)
		fmt.Println("OK")

		return nil
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
		return fmt.Errorf("error when getting hooks payload: %v", err)
	}

	hookCommand, ok := hooksBySubcommand[subCmd]
	if !ok {
		return fmt.Errorf("subcommand name invalid: %q", subCmd)
	}

	// If the hook wasn't requested, then we simply skip executing any
	// logic.
	if !payload.IsHookRequested(hookCommand.hookType) {
		return nil
	}

	conn, err := dialGitaly(payload)
	if err != nil {
		return fmt.Errorf("error when connecting to gitaly: %v", err)
	}
	defer conn.Close()

	hookClient := gitalypb.NewHookServiceClient(conn)

	ctx = featureflag.OutgoingWithRaw(ctx, payload.FeatureFlags)
	if err := hookCommand.exec(ctx, payload, hookClient, args); err != nil {
		return err
	}

	return nil
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

	gitPushOptionCount, err := env.GetInt("GIT_PUSH_OPTION_COUNT", 0)
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
		errClose := stream.CloseSend()
		if errSend != nil {
			errC <- errSend
		} else {
			errC <- errClose
		}
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

	gitlabAPI, err := gitlab.NewHTTPClient(logrus.New(), cfg.Gitlab, cfg.TLS, prometheus.Config{})
	if err != nil {
		return nil, err
	}

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return hook.NewManager(cfg, config.NewLocator(cfg), gitCmdFactory, nil, gitlabAPI).Check(context.TODO())
}

func updateHook(ctx context.Context, payload git.HooksPayload, hookClient gitalypb.HookServiceClient, args []string) error {
	args = args[2:]
	if len(args) != 3 {
		return fmt.Errorf("update hook expects exactly three arguments, got %q", args)
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
		return fmt.Errorf("error when starting command for update hook: %v", err)
	}

	if returnCode, err := stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return updateHookStream.Recv()
	}, noopSender, os.Stdout, os.Stderr); err != nil {
		return fmt.Errorf("error when receiving data for update hook: %v", err)
	} else if returnCode != 0 {
		return hookError{returnCode: int(returnCode)}
	}

	return nil
}

func preReceiveHook(ctx context.Context, payload git.HooksPayload, hookClient gitalypb.HookServiceClient, args []string) error {
	preReceiveHookStream, err := hookClient.PreReceiveHook(ctx)
	if err != nil {
		return fmt.Errorf("error when getting preReceiveHookStream client for: %v", err)
	}

	if err := preReceiveHookStream.Send(&gitalypb.PreReceiveHookRequest{
		Repository:           payload.Repo,
		EnvironmentVariables: os.Environ(),
		GitPushOptions:       gitPushOptions(),
	}); err != nil {
		return fmt.Errorf("error when sending request for pre-receive hook: %v", err)
	}

	f := sendFunc(streamio.NewWriter(func(p []byte) error {
		return preReceiveHookStream.Send(&gitalypb.PreReceiveHookRequest{Stdin: p})
	}), preReceiveHookStream, os.Stdin)

	if returnCode, err := stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return preReceiveHookStream.Recv()
	}, f, os.Stdout, os.Stderr); err != nil {
		return fmt.Errorf("error when receiving data for pre-receive hook: %v", err)
	} else if returnCode != 0 {
		return hookError{returnCode: int(returnCode)}
	}

	return nil
}

func postReceiveHook(ctx context.Context, payload git.HooksPayload, hookClient gitalypb.HookServiceClient, args []string) error {
	postReceiveHookStream, err := hookClient.PostReceiveHook(ctx)
	if err != nil {
		return fmt.Errorf("error when getting stream client for post-receive hook: %v", err)
	}

	if err := postReceiveHookStream.Send(&gitalypb.PostReceiveHookRequest{
		Repository:           payload.Repo,
		EnvironmentVariables: os.Environ(),
		GitPushOptions:       gitPushOptions(),
	}); err != nil {
		return fmt.Errorf("error when sending request for post-receive hook: %v", err)
	}

	f := sendFunc(streamio.NewWriter(func(p []byte) error {
		return postReceiveHookStream.Send(&gitalypb.PostReceiveHookRequest{Stdin: p})
	}), postReceiveHookStream, os.Stdin)

	if returnCode, err := stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return postReceiveHookStream.Recv()
	}, f, os.Stdout, os.Stderr); err != nil {
		return fmt.Errorf("error when receiving data for post-receive hook: %v", err)
	} else if returnCode != 0 {
		return hookError{returnCode: int(returnCode)}
	}

	return nil
}

func referenceTransactionHook(ctx context.Context, payload git.HooksPayload, hookClient gitalypb.HookServiceClient, args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("reference-transaction hook is missing required arguments, got %q", args)
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
		return fmt.Errorf("reference-transaction hook has invalid state: %q", args[2])
	}

	referenceTransactionHookStream, err := hookClient.ReferenceTransactionHook(ctx)
	if err != nil {
		return fmt.Errorf("error when getting referenceTransactionHookStream client: %v", err)
	}

	if err := referenceTransactionHookStream.Send(&gitalypb.ReferenceTransactionHookRequest{
		Repository:           payload.Repo,
		EnvironmentVariables: os.Environ(),
		State:                state,
	}); err != nil {
		return fmt.Errorf("error when sending request for reference-transaction hook: %v", err)
	}

	f := sendFunc(streamio.NewWriter(func(p []byte) error {
		return referenceTransactionHookStream.Send(&gitalypb.ReferenceTransactionHookRequest{Stdin: p})
	}), referenceTransactionHookStream, os.Stdin)

	if returnCode, err := stream.Handler(func() (stream.StdoutStderrResponse, error) {
		return referenceTransactionHookStream.Recv()
	}, f, os.Stdout, os.Stderr); err != nil {
		return fmt.Errorf("error when receiving data for reference-transaction hook: %v", err)
	} else if returnCode != 0 {
		return hookError{returnCode: int(returnCode)}
	}

	return nil
}

func packObjectsHook(ctx context.Context, payload git.HooksPayload, hookClient gitalypb.HookServiceClient, args []string) error {
	if err := handlePackObjectsWithSidechannel(ctx, hookClient, payload.Repo, args[2:]); err != nil {
		return hookError{returnCode: 1, err: fmt.Errorf("RPC failed: %w", err)}
	}

	return nil
}

func handlePackObjectsWithSidechannel(ctx context.Context, hookClient gitalypb.HookServiceClient, repo *gitalypb.Repository, args []string) error {
	ctx, wt, err := hook.SetupSidechannel(ctx, func(c *net.UnixConn) error {
		// We don't have to worry about concurrent reads and writes and
		// deadlocks, because we're connected to git-upload-pack which follows
		// the sequence: (1) write to stdin of pack-objects, (2) close stdin of
		// pack-objects, (3) concurrently read from stdout and stderr of
		// pack-objects.
		if _, err := io.Copy(c, os.Stdin); err != nil {
			return fmt.Errorf("copy stdin: %w", err)
		}
		if err := c.CloseWrite(); err != nil {
			return fmt.Errorf("close write: %w", err)
		}

		if err := pktline.EachSidebandPacket(c, func(band byte, data []byte) error {
			var err error
			switch band {
			case 1:
				_, err = os.Stdout.Write(data)
			case 2:
				_, err = os.Stderr.Write(data)
			default:
				err = fmt.Errorf("unexpected side band: %d", band)
			}
			return err
		}); err != nil {
			return fmt.Errorf("demux response: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("SetupSidechannel: %w", err)
	}
	defer wt.Close()

	if _, err := hookClient.PackObjectsHookWithSidechannel(
		ctx,
		&gitalypb.PackObjectsHookWithSidechannelRequest{Repository: repo, Args: args},
	); err != nil {
		return fmt.Errorf("call PackObjectsHookWithSidechannel: %w", err)
	}

	return wt.Wait()
}
