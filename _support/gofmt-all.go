package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) != 2 {
		printUsage()
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "-n":
		err = gofmt(false)
	case "-f":
		err = gofmt(true)
	default:
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
}

func gofmt(write bool) error {
	goFiles, err := findAllGoFiles()
	if err != nil {
		return err
	}

	args := []string{"gofmt", "-s", "-l"}
	if write {
		args = append(args, "-w")
	}
	args = append(args, goFiles...)
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stderr = os.Stderr

	output, err := cmd.Output()
	if err != nil {
		return err
	}
	fmt.Printf("%s", output)

	if !write && len(output) > 0 {
		return fmt.Errorf("Please run './run format'")
	}
	return nil
}

func printUsage() {
	fmt.Println(`Usage: gofmt-all.go [-n | -f]

gofmt-all.go runs 'gofmt -s' on all non-vendored .go files in this project.
  -n  List files that would be changed
  -f  Apply changes
`)
}

func findAllGoFiles() ([]string, error) {
	// Unfortunately 'gofmt -s' only works on single files, so we must build
	// a list of all non-vendored Go files.
	goFiles := []string{}
	walkFunc := func(p string, info os.FileInfo, err error) error {
		if info.Mode().IsDir() {
			if p == "vendor" || strings.HasPrefix(p, ".") || strings.HasPrefix(p, "_") {
				if p != "." && p != "_support" {
					return filepath.SkipDir
				}
			}
		}

		if info.Mode().IsRegular() && strings.HasSuffix(p, ".go") {
			// This works because this function is defined inside the function where
			// goFiles was declared (closure).
			goFiles = append(goFiles, p)
		}
		return nil
	}
	if err := filepath.Walk(".", walkFunc); err != nil {
		return nil, err
	}
	return goFiles, nil
}
