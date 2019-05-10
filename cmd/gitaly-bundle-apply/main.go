package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
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

	line, err := readLine(r)
	if err != nil {
		return err
	}

	const header = "# gitaly bundle v1"
	if line != header {
		return fmt.Errorf("invalid header %q", line)
	}

	// Ref dump is at the head of the bundle so we must consume it first
	newRefs, err := refMapFromReader(r)
	if err != nil {
		return err
	}

	// There may not be a pack file, for instance if the only change is a ref deletion.
	const magic = "PACK"
	if b, err := r.Peek(len(magic)); err == nil && string(b) == magic {
		// TODO use --keep to prevent concurrent repack/gc from deleting this
		// packfile before we apply the ref update below?
		indexPack := exec.Command("git", "index-pack", "--stdin", "--fix-thin")
		indexPack.Stdin = r
		indexPack.Stdout = os.Stderr
		indexPack.Stderr = os.Stderr

		if err := indexPack.Run(); err != nil {
			return err
		}
	}

	oldRefs, err := currentRefs()
	if err != nil {
		return err
	}

	refUpdate := &bytes.Buffer{}
	for ref, oid := range newRefs {
		fmt.Fprintf(refUpdate, "update %s %s\n", ref, oid)
	}

	for ref, _ := range oldRefs {
		if _, ok := newRefs[ref]; ok {
			continue
		}
		fmt.Fprintf(refUpdate, "delete %s\n", ref)
	}

	updateRef := exec.Command("git", "update-ref", "--stdin")
	updateRef.Stdin = refUpdate
	updateRef.Stdout = os.Stderr
	updateRef.Stderr = os.Stderr

	if err := updateRef.Run(); err != nil {
		return err
	}

	return nil
}

func readLine(r *bufio.Reader) (string, error) {
	b, err := r.ReadBytes('\n')
	if err != nil {
		return "", err
	}

	return string(b)[:len(b)-1], err
}

func currentRefs() (map[string]string, error) {
	cmd := exec.Command("git", "for-each-ref", "--format=%(objectname) %(refname)")
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	refs, err := refMapFromReader(bufio.NewReader(bytes.NewReader(out)))
	if err == io.EOF {
		err = nil
	}

	return refs, err
}

func refMapFromReader(r *bufio.Reader) (map[string]string, error) {
	result := make(map[string]string)
	var err error

	for {
		line, err := readLine(r)
		if err != nil || line == "" {
			break
		}

		split := strings.SplitN(line, " ", 2)
		if len(split) != 2 {
			return nil, fmt.Errorf("invalid ref line %q", line)
		}
		result[split[1]] = split[0]
	}

	return result, err
}
