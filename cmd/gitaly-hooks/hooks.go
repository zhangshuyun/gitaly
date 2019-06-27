package main

import (
	"context"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/sirupsen/logrus"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/log"
)

var logger *logrus.Logger

func init() {
	var err error

	logger, err = log.NewHookLogger(filepath.Join(os.Getenv("GITALY_LOG_DIR"), "gitlab_shell.log"))
	if err != nil {
		logger = logrus.New()
		logger.SetOutput(ioutil.Discard)
	}
}

func main() {
	logger.Error("WAWAWAWA")
	if len(os.Args) < 2 {
		logger.Fatal("requires hook name")
	}

	gitlabRubyDir := os.Getenv("GITALY_RUBY_DIR")
	if gitlabRubyDir == "" {
		logger.Fatal("GITALY_RUBY_DIR not set")
	}

	hookName := os.Args[1]
	rubyHookPath := filepath.Join(gitlabRubyDir, "gitlab-shell", "hooks", hookName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var hookCmd *exec.Cmd

	switch hookName {
	case "update":
		args := os.Args[1:]
		if len(args) != 3 {
			logger.Fatal("update hook missing required arguments")
		}

		hookCmd = exec.Command(rubyHookPath, args[:3]...)
	case "pre-receive", "post-receive":
		hookCmd = exec.Command(rubyHookPath)
	default:
		logger.Fatal("hook name invalid")
	}

	cmd, err := command.New(ctx, hookCmd, os.Stdin, os.Stdout, os.Stderr, os.Environ()...)
	if err != nil {
		logger.Fatalf("error when starting command for %v: %v", rubyHookPath, err)
	}

	if err = cmd.Wait(); err != nil {
		logger.Fatalf("error when executing command for %v: %v", rubyHookPath, err)
	}
}
