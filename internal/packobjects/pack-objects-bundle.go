package packobjects

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
)

const bundleFileName = "gitaly/clone.bundle"

var shaRegex = regexp.MustCompile(`\A[0-9a-f]{40}\z`)

func PackObjects(ctx context.Context, cwd string, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	request := &bytes.Buffer{}
	scanner := bufio.NewScanner(io.TeeReader(stdin, request))
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

	// TODO check args. If unexpected, return fallback.

	if !isClone {
		return fallback(ctx, args, request, stdout, stderr)
	}

	bundleFile, err := os.Open(filepath.Join(cwd, bundleFileName))
	if err != nil {
		return fallback(ctx, args, request, stdout, stderr)
	}
	defer bundleFile.Close()

	bundle := bufio.NewReader(bundleFile)

	request = bytes.NewBuffer(bytes.TrimSpace(request.Bytes()))
	if _, err := request.WriteString("\n"); err != nil {
		return err
	}

	if err := addBundleRefsToRequest(request, bundle); err != nil {
		return err
	}

	bundleReader, err := git.NewPackReader(bundle)
	if err != nil {
		return err
	}

	cmd, err := command.New(ctx, exec.Command(args[0], args[1:]...), request, nil, stderr)
	if err != nil {
		return err
	}

	packObjectsReader, err := git.NewPackReader(cmd)
	if err != nil {
		return err
	}

	// TODO check for overflow
	totalObjects := packObjectsReader.NumObjects() + bundleReader.NumObjects()

	w, err := git.NewPackWriter(stdout, totalObjects)
	if err != nil {
		return err
	}

	if _, err := io.Copy(w, packObjectsReader); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	fmt.Fprintf(stderr, "Pre-computed packfile: %d objects\n", bundleReader.NumObjects())

	if _, err := io.Copy(w, bundleReader); err != nil {
		return err
	}

	if err := w.Flush(); err != nil {
		return err
	}

	return nil
}

func fallback(ctx context.Context, args []string, request io.Reader, stdout, stderr io.Writer) error {
	cmd, err := command.New(ctx, exec.Command(args[0], args[1:]...), request, stdout, stderr)
	if err != nil {
		return err
	}

	return cmd.Wait()
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return "", err
	}

	return string(line[:len(line)-1]), nil
}

const BundleHeader = "# v2 git bundle"

func addBundleRefsToRequest(request io.Writer, bundle *bufio.Reader) error {
	bundleHeader, err := readLine(bundle)
	if err != nil {
		return err
	}
	if bundleHeader != BundleHeader {
		return fmt.Errorf("unexpected bundle header: %q", bundleHeader)
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

	return nil
}
