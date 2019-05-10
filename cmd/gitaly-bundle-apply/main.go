package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
)

func main() {
	if err := _main(); err != nil {
		log.Fatal(err)
	}
}

func _main() error {
	r := bufio.NewReader(os.Stdin)

	const header = "# gitaly bundle v1"
	line, err := readLine(r)
	if err != nil {
		return err
	}
	if line != header {
		return fmt.Errorf("invalid header %q", line)
	}

	refUpdate := &bytes.Buffer{}
	for {
		line, err := readLine(r)
		if err != nil {
			return err
		}
		if line == "" {
			break
		}

		split := strings.SplitN(line, " ", 2)
		if len(split) != 2 {
			return fmt.Errorf("invalid ref line %q", line)
		}
		if _, err := fmt.Fprintf(refUpdate, "update %s %s\n", split[1], split[0]); err != nil {
			return err
		}
	}

	indexPack := exec.Command("git", "index-pack", "--stdin", "--fix-thin")
	indexPack.Stdin = r
	indexPack.Stdout = os.Stderr
	indexPack.Stderr = os.Stderr

	if err := indexPack.Start(); err != nil {
		return err
	}
	if err := indexPack.Wait(); err != nil {
		return err
	}

	updateRef := exec.Command("git", "update-ref", "--stdin")
	updateRef.Stdin = refUpdate
	updateRef.Stdout = os.Stderr
	updateRef.Stderr = os.Stderr

	if err := updateRef.Start(); err != nil {
		return err
	}
	if err := updateRef.Wait(); err != nil {
		return err
	}

	return nil
}

func readLine(r *bufio.Reader) (string, error) {
	b, err := r.ReadBytes('\n')
	return string(b)[:len(b)-1], err
}
