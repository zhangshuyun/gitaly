package main

import (
	"log"
	"os"
	"strings"
	"time"

	"golang.org/x/net/context"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/client"
)

func main() {
	if !(len(os.Args) >= 3 && strings.HasPrefix(os.Args[2], "git-receive-pack")) {
		log.Fatalf("Not a valid command")
	}

	addr := os.Getenv("GITALY_SOCKET")
	if len(addr) == 0 {
		log.Fatalf("GITALY_SOCKET not set")
	}

	cli, err := client.NewClient(addr)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer cli.Close()

	req := &pb.SSHReceivePackRequest{
		Repository: &pb.Repository{Path: os.Getenv("GL_REPOSITORY")},
		GlId:       os.Getenv("GL_ID"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	code, err := cli.ReceivePack(ctx, os.Stdin, os.Stdout, os.Stderr, req)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	os.Exit(int(code))
}
