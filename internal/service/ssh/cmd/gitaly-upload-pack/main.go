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
	if !(len(os.Args) >= 3 && strings.HasPrefix(os.Args[2], "git-upload-pack")) {
		log.Fatalf("Not a valid command")
	}

	addr := os.Getenv("GITALY_SOCKET")
	if len(addr) == 0 {
		log.Fatalf("GITALY_SOCKET not set")
	}

	conn, err := client.Dial(addr, client.DefaultDialOpts)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	defer conn.Close()

	req := &pb.SSHUploadPackRequest{
		Repository: &pb.Repository{Path: os.Getenv("GL_REPOSITORY")},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	code, err := client.UploadPack(conn, ctx, os.Stdin, os.Stdout, os.Stderr, req)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	os.Exit(int(code))
}
