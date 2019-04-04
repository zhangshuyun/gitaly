package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("not enough argument to pack-objects hook")
	}

	if err := _main(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

var shaRegex = regexp.MustCompile(`\A[0-9a-f]{40}\z`)

func _main(packObjects []string) error {
	request := &bytes.Buffer{}
	scanner := bufio.NewScanner(io.TeeReader(os.Stdin, request))
	seenNot := false
	isClone := true
	for scanner.Scan() {
		if !seenNot && scanner.Text() == "--not" {
			seenNot = true
			continue
		}

		if seenNot && scanner.Text() != "" {
			isClone = false
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if !isClone {
		return fallback(packObjects, request)
	}

	bundleFile, err := os.Open("clone.bundle")
	if err != nil {
		return fallback(packObjects, request)
	}
	defer bundleFile.Close()

	bundle := bufio.NewReader(bundleFile)
	bundleHeader, err := readLine(bundle)
	if err != nil {
		return err
	}
	if bundleHeader != "# v2 git bundle" {
		return fmt.Errorf("unexpected bundle header: %q", bundleHeader)
	}

	request = bytes.NewBuffer(bytes.TrimSpace(request.Bytes()))
	if _, err := request.WriteString("\n"); err != nil {
		return err
	}

	for {
		refLine, err := readLine(bundle)
		if err != nil {
			return err
		}

		if refLine == "" {
			break
		}

		split := strings.SplitN(refLine, " ", 2)
		if len(split) != 2 {
			return fmt.Errorf("invalid ref line: %q", refLine)
		}
		id := split[0]
		if !shaRegex.MatchString(id) {
			return fmt.Errorf("invalid object ID: %q", id)
		}

		if _, err := fmt.Fprintln(request, id); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, packObjects[0], packObjects[1:]...)
	cmd.Stdin = request
	cmd.Stderr = os.Stderr

	packObjectsOut, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	packObjectsReader, err := NewPackReader(packObjectsOut)
	if err != nil {
		return err
	}

	bundleReader, err := NewPackReader(bundle)
	if err != nil {
		return err
	}

	summer := sha1.New()
	stdout := io.MultiWriter(os.Stdout, summer)

	if _, err := fmt.Fprint(stdout, packMagic); err != nil {
		return err
	}

	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, packObjectsReader.NumObjects()+bundleReader.NumObjects()) // TODO check for overflow
	if _, err := stdout.Write(size); err != nil {
		return err
	}

	if _, err := io.Copy(stdout, packObjectsReader); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	if _, err := io.Copy(stdout, bundleReader); err != nil {
		return err
	}

	if _, err := stdout.Write(summer.Sum(nil)); err != nil {
		return err
	}

	return nil
}

func fallback(packObjects []string, request io.Reader) error {
	cmd := exec.Command(packObjects[0], packObjects[1:]...)
	cmd.Stdin = request
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return "", err
	}

	return string(line[:len(line)-1]), nil
}
