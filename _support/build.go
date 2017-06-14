package main

import (
	"build"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	config, err := build.ReadConfig()
	if err != nil {
		return err
	}

	binDir := path.Join(config.BuildDir, "bin")
	if err := os.RemoveAll(binDir); err != nil {
		return err
	}
	version, err := build.Version()
	if err != nil {
		return err
	}
	goInstall := exec.Command("go", "install", "-ldflags", "-X main.version="+version, config.Package+"/cmd/...")
	goInstall.Stdout = os.Stdout
	goInstall.Stderr = os.Stderr
	env := []string{}
	for _, s := range os.Environ() {
		if strings.HasPrefix(s, "GOPATH=") {
			continue
		}
		env = append(env, s)
	}
	goInstall.Env = append(env, "GOPATH="+config.BuildGopath)
	if err := goInstall.Run(); err != nil {
		return err
	}
	cp := exec.Command("sh", "-c", fmt.Sprintf("cp %s/* .", binDir))
	cp.Stdout = os.Stdout
	cp.Stderr = os.Stderr
	return cp.Run()
}
