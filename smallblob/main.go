package main

import (
	"bufio"
	"fmt"
	"gopkg.in/libgit2/git2go.v27"
	"log"
	"os"
	"os/exec"
	"strings"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("usage: %s GIT_DIR", os.Args[0])
	}

	gitDir := os.Args[1]
	revList := exec.Command("git", "rev-list", "--objects", "--all", "--use-bitmap-index")
	revList.Dir = gitDir
	revListOut, err := revList.StdoutPipe()
	noErr(err)
	noErr(revList.Start())

	repo, err := git.OpenRepository(gitDir)
	noErr(err)

	odb, err := repo.Odb()
	noErr(err)

	out := bufio.NewWriter(os.Stdout)
	scanner := bufio.NewScanner(revListOut)
	for scanner.Scan() {
		split := strings.Fields(scanner.Text())
		if len(split) == 0 {
			log.Fatalf("no fields in line %q", scanner.Text())
		}

		oid, err := git.NewOid(split[0])
		noErr(err)

		s, t, err := odb.ReadHeader(oid)
		noErr(err)

		if s < 100 || s > 200 || t != git.ObjectBlob {
			continue
		}

		obj, err := odb.Read(oid)
		noErr(err)

		fmt.Fprintf(out, "%s blob %d\n%s\n", oid, s, obj.Data())
		obj.Free()
	}

	noErr(scanner.Err())
	noErr(revList.Wait())
	noErr(out.Flush())
}

func noErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
