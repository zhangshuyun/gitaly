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

	"github.com/BurntSushi/toml"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitlabshell"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc"
)

func main() {
	var logger = gitalylog.NewHookLogger()

	if len(os.Args) < 2 {
		logger.Fatal(errors.New("requires hook name"))
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

	gitalySocket, ok := os.LookupEnv("GITALY_SOCKET")
	if !ok {
		logger.Fatal(errors.New("GITALY_SOCKET not set"))
	}

	conn, err := client.Dial("unix://"+gitalySocket, dialOpts())
	if err != nil {
		logger.Fatalf("error when dialing: %v", err)
	}

	client := gitalypb.NewHookServiceClient(conn)

	var hookStatus int32

	switch subCmd {
	case "update":
		args := os.Args[2:]
		if len(args) != 3 {
			logger.Fatal(errors.New("update hook missing required arguments"))
		}
		ref, oldValue, newValue := args[0], args[1], args[2]

		req := &gitalypb.UpdateHookRequest{
			Repository: &gitalypb.Repository{
				StorageName:  os.Getenv("GL_REPO_STORAGE"),
				RelativePath: os.Getenv("GL_REPO_RELATIVE_PATH"),
				GlRepository: os.Getenv("GL_REPOSITORY"),
			},
			KeyId:    os.Getenv("GL_ID"),
			Ref:      []byte(ref),
			OldValue: oldValue,
			NewValue: newValue,
		}

		stream, err := client.UpdateHook(ctx, req)
		if err != nil {
			logger.Fatalf("error when starting command for %v: %v", subCmd, err)
		}

		if hookStatus, err = sendAndRecv(stream, new(gitalypb.UpdateHookResponse), nil, os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %v: %v", subCmd, err)
		}
	case "pre-receive":
		stream, err := client.PreReceiveHook(ctx)
		if err != nil {
			logger.Fatalf("error when getting stream client for %v: %v", subCmd, err)
		}

		if err := stream.Send(&gitalypb.PreReceiveHookRequest{
			Repository: &gitalypb.Repository{
				StorageName:  os.Getenv("GL_REPO_STORAGE"),
				RelativePath: os.Getenv("GL_REPO_RELATIVE_PATH"),
				GlRepository: os.Getenv("GL_REPOSITORY"),
			},
			KeyId:    os.Getenv("GL_ID"),
			Protocol: os.Getenv("GL_PROTOCOL"),
		}); err != nil {
			logger.Fatalf("error when sending request for %v: %v", subCmd, err)
		}

		f := sendFunc(streamio.NewWriter(func(p []byte) error {
			return stream.Send(&gitalypb.PreReceiveHookRequest{Stdin: p})
		}), stream, os.Stdin)

		if hookStatus, err = sendAndRecv(stream, new(gitalypb.PreReceiveHookResponse), f, os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %v: %v", subCmd, err)
		}
	case "post-receive":
		stream, err := client.PostReceiveHook(ctx)
		if err != nil {
			logger.Fatalf("error when getting stream client for %v: %v", subCmd, err)
		}

		if err := stream.Send(&gitalypb.PostReceiveHookRequest{
			Repository: &gitalypb.Repository{
				StorageName:  os.Getenv("GL_REPO_STORAGE"),
				RelativePath: os.Getenv("GL_REPO_RELATIVE_PATH"),
				GlRepository: os.Getenv("GL_REPOSITORY"),
			},
			KeyId:          os.Getenv("GL_ID"),
			GitPushOptions: gitPushOptions(),
		}); err != nil {
			logger.Fatalf("error when sending request for %v: %v", subCmd, err)
		}

		f := sendFunc(streamio.NewWriter(func(p []byte) error {
			return stream.Send(&gitalypb.PostReceiveHookRequest{Stdin: p})
		}), stream, os.Stdin)

		if hookStatus, err = sendAndRecv(stream, new(gitalypb.PostReceiveHookResponse), f, os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %v: %v", subCmd, err)
		}
	default:
		logger.Fatal(fmt.Errorf("subcommand name invalid: %v", subCmd))
	}

	os.Exit(int(hookStatus))
}

func gitPushOptions() []string {
	gitPushOptionCount, err := strconv.Atoi(os.Getenv("GIT_PUSH_OPTION_COUNT"))
	if err != nil {
		return []string{}
	}

	var gitPushOptions []string

	for i := 0; i < gitPushOptionCount; i++ {
		gitPushOptions = append(gitPushOptions, os.Getenv(fmt.Sprintf("GIT_PUSH_OPTION_%d", i)))
	}

	return gitPushOptions
}

func dialOpts() []grpc.DialOption {
	return append(client.DefaultDialOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(os.Getenv("GITALY_TOKEN"))))
}

func sendFunc(reqWriter io.Writer, stream grpc.ClientStream, stdin io.Reader) func(errC chan error) {
	return func(errC chan error) {
		_, errSend := io.Copy(reqWriter, stdin)
		stream.CloseSend()
		errC <- errSend
	}
}

func sendAndRecv(stream grpc.ClientStream, resp client.StdoutStderrResponse, sender client.Sender, stdout, stderr io.Writer) (int32, error) {
	if sender == nil {
		sender = func(err chan error) {}
	}
	return client.StreamHandler(func() (client.StdoutStderrResponse, error) {
		err := stream.RecvMsg(resp)
		return resp, err
	}, sender, stdout, stderr)
}

// GitlabShellConfig contains a subset of gitlabshell's config.yml
type GitlabShellConfig struct {
	GitlabURL    string       `yaml:"gitlab_url"`
	HTTPSettings HTTPSettings `yaml:"http_settings"`
}

// HTTPSettings contains fields for http settings
type HTTPSettings struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
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
	cmd.Env = append(os.Environ(), gitlabshell.EnvFromConfig(c)...)

	if err = cmd.Run(); err != nil {
		if status, ok := command.ExitStatus(err); ok {
			return status, nil
		}
		return 1, err
	}

	return 0, nil
}
