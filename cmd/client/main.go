package main

import (
	"os"

	"gitlab.com/gitlab-org/git-access-daemon/client"
)

func main() {
	client := client.NewClient("127.0.0.1:6666")
	defer client.Close()

	exitStatus := client.Run(os.Args)
	os.Exit(exitStatus)
}
