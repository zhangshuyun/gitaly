package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

func main() {
	var logger = log.NewHookLogger()

	if len(os.Args) < 2 {
		logger.Fatal(errors.New("requires hook name"))
	}

	subCmd := os.Args[1]

	if subCmd == "check" {
		configPath := os.Args[2]

		if err := checkGitlabAccess(configPath); err != nil {
			os.Stderr.WriteString(err.Error())
			os.Exit(1)
		}

		os.Stdout.WriteString("OK")
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := client.Dial(os.Getenv("GITALY_SOCKET"), dialOpts())
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
			logger.Fatalf("error when getting stream client: %v", err)
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
			logger.Fatalf("error when sending request: %v", err)
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
			logger.Fatalf("error when getting stream client: %v", err)
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
			logger.Fatalf("error when sending request: %v", err)
		}

		f := sendFunc(streamio.NewWriter(func(p []byte) error {
			return stream.Send(&gitalypb.PostReceiveHookRequest{Stdin: p})
		}), stream, os.Stdin)

		if hookStatus, err = sendAndRecv(stream, new(gitalypb.PostReceiveHookResponse), f, os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %v: %v", subCmd, err)
		}
	default:
		logger.Fatal(errors.New("subcommand name invalid"))
	}

	os.Exit(int(hookStatus))
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

func checkGitlabAccess(configPath string) error {
	cfgFile, err := os.Open(configPath)
	if err != nil {
		return fmt.Errorf("error when opening config file: %v", err)
	}
	defer cfgFile.Close()

	config := GitlabShellConfig{}

	if err := yaml.NewDecoder(cfgFile).Decode(&config); err != nil {
		return fmt.Errorf("load toml: %v", err)
	}

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v4/internal/check", strings.TrimRight(config.GitlabURL, "/")), nil)
	if err != nil {
		return fmt.Errorf("could not create request for %s: %v", config.GitlabURL, err)
	}

	req.SetBasicAuth(config.HTTPSettings.User, config.HTTPSettings.Password)

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("error with request for %s: %v", config.GitlabURL, err)
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("FAILED. code: %d", resp.StatusCode)
	}

	return nil
}
