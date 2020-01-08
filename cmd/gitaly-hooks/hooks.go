package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
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

	c := gitalypb.NewHookServiceClient(conn)

	var hookSuccess bool

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

		stream, err := c.UpdateHook(ctx, req)
		if err != nil {
			logger.Fatalf("error when starting command for %v: %v", subCmd, err)
		}

		if hookSuccess, err = recvHookResponse(stream, new(gitalypb.UpdateHookResponse), os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %v: %v", subCmd, err)
		}
	case "pre-receive":
		stream, err := c.PreReceiveHook(ctx)
		if err != nil {
			logger.Fatalf("error when getting stream client: %v", err)
		}

		if err = sendRequest(stream, preReceiveHookRequest, os.Stdin); err != nil {
			logger.Fatalf("error when sending data for %v: %v", subCmd, err)
		}
		if err = stream.CloseSend(); err != nil {
			logger.Fatalf("error when closing sending stream for %v: %v", subCmd, err)
		}

		if hookSuccess, err = recvHookResponse(stream, new(gitalypb.PreReceiveHookResponse), os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %v: %v", subCmd, err)
		}
	case "post-receive":
		stream, err := c.PostReceiveHook(ctx)
		if err != nil {
			logger.Fatalf("error when getting stream client: %v", err)
		}

		if err = sendRequest(stream, postReceiveHookRequest, os.Stdin); err != nil {
			logger.Fatalf("error when sending data for %v: %v", subCmd, err)
		}
		if err = stream.CloseSend(); err != nil {
			logger.Fatalf("error when closing sending stream for %v: %v", subCmd, err)
		}

		if hookSuccess, err = recvHookResponse(stream, new(gitalypb.PostReceiveHookResponse), os.Stdout, os.Stderr); err != nil {
			logger.Fatalf("error when receiving data for %v: %v", subCmd, err)
		}
	default:
		logger.Fatal(errors.New("subcommand name invalid"))
	}

	if !hookSuccess {
		os.Exit(1)
	}

	os.Exit(0)
}

func dialOpts() []grpc.DialOption {
	return append(client.DefaultDialOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(os.Getenv("GITALY_TOKEN"))))
}

// streamRequestGenerator is a function that generates requests for sending to a stream
type streamRequestGenerator func(firstRequest bool, p []byte) interface{}

// SendRequest streams requests using a reader
func sendRequest(stream grpc.ClientStream, g streamRequestGenerator, r io.Reader) error {
	if err := stream.SendMsg(g(true, nil)); err != nil {
		return err
	}

	w := streamio.NewWriter(func(p []byte) error {
		return stream.SendMsg(g(false, p))
	})

	if _, err := io.Copy(w, r); err != nil {
		return err
	}

	return stream.CloseSend()
}

func postReceiveHookRequest(first bool, p []byte) interface{} {
	if first {
		return &gitalypb.PostReceiveHookRequest{
			Repository: &gitalypb.Repository{
				StorageName:  os.Getenv("GL_REPO_STORAGE"),
				RelativePath: os.Getenv("GL_REPO_RELATIVE_PATH"),
				GlRepository: os.Getenv("GL_REPOSITORY"),
			},
			KeyId: os.Getenv("GL_ID"),
		}
	}

	return &gitalypb.PostReceiveHookRequest{Stdin: p}
}

func preReceiveHookRequest(first bool, p []byte) interface{} {
	if first {
		return &gitalypb.PreReceiveHookRequest{
			Repository: &gitalypb.Repository{
				StorageName:  os.Getenv("GL_REPO_STORAGE"),
				RelativePath: os.Getenv("GL_REPO_RELATIVE_PATH"),
				GlRepository: os.Getenv("GL_REPOSITORY"),
			},
			KeyId:    os.Getenv("GL_ID"),
			Protocol: os.Getenv("GL_PROTOCOL"),
		}
	}
	return &gitalypb.PreReceiveHookRequest{Stdin: p}
}

type hookResponse interface {
	GetStdout() []byte
	GetStderr() []byte
	GetSuccess() bool
}

func recvHookResponse(stream grpc.ClientStream, resp hookResponse, stdout, stderr io.Writer) (bool, error) {
	var err error
	var success bool
	for {
		err = stream.RecvMsg(resp)
		if err != nil {
			break
		}

		if _, err = stdout.Write(resp.GetStdout()); err != nil {
			return false, err
		}
		if _, err = stderr.Write(resp.GetStderr()); err != nil {
			return false, err
		}
		success = resp.GetSuccess()
	}

	if err != io.EOF {
		return false, err
	}

	return success, nil
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
