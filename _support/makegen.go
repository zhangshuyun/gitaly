/*
	makegen.go -- Makefile generator for Gitaly

This file is used to generate _build/Makefile. In _build/Makefile we
can assume that we are in a GOPATH (rooted at _build) and that
$GOPATH/bin is in PATH. The generator runs in the root of the Gitaly
tree. The goal of the generator is to use as little dynamic behaviors
in _build/Makefile (e.g. shelling out to find a list of files), and do
these things as much as possible in Go and then pass them into the
template.

The working directory of makegen.go and the Makefile it generates is
_build.
*/

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"text/template"
	"time"
)

func main() {
	gm := &gitalyMake{}

	tmpl := template.New("Makefile")
	tmpl.Funcs(map[string]interface{}{
		"join": strings.Join,
	})
	tmpl = template.Must(tmpl.Parse(templateText))

	err := tmpl.Execute(os.Stdout, gm)
	if err != nil {
		log.Fatalf("execution failed: %s", err)
	}
}

type gitalyMake struct {
	commandPackages []string
	cwd             string
	versionPrefixed string
	goFiles         []string
	buildTime       string
}

// BuildDir is the GOPATH root. It is also the working directory of the Makefile we are generating.
func (gm *gitalyMake) BuildDir() string {
	if len(gm.cwd) > 0 {
		return gm.cwd
	}

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	gm.cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		log.Fatal(err)
	}

	return gm.cwd
}

func (gm *gitalyMake) Pkg() string               { return "gitlab.com/gitlab-org/gitaly" }
func (gm *gitalyMake) GoImports() string         { return "bin/goimports" }
func (gm *gitalyMake) GoCovMerge() string        { return "bin/gocovmerge" }
func (gm *gitalyMake) GoLint() string            { return "bin/golint" }
func (gm *gitalyMake) GoVendor() string          { return "bin/govendor" }
func (gm *gitalyMake) StaticCheck() string       { return filepath.Join(gm.BuildDir(), "bin/staticcheck") }
func (gm *gitalyMake) CoverageDir() string       { return filepath.Join(gm.BuildDir(), "cover") }
func (gm *gitalyMake) GitalyRubyDir() string     { return filepath.Join(gm.SourceDir(), "ruby") }
func (gm *gitalyMake) GitlabShellRelDir() string { return "ruby/vendor/gitlab-shell" }
func (gm *gitalyMake) GitlabShellDir() string {
	return filepath.Join(gm.SourceDir(), gm.GitlabShellRelDir())
}

// SourceDir is the location of gitaly's files, inside the _build GOPATH.
func (gm *gitalyMake) SourceDir() string { return filepath.Join(gm.BuildDir(), "src", gm.Pkg()) }

func (gm *gitalyMake) TestRepoStoragePath() string {
	path := os.Getenv("TEST_REPO_STORAGE_PATH")
	if len(path) == 0 {
		log.Fatal("TEST_REPO_STORAGE_PATH is not set")
	}

	return path
}

func (gm *gitalyMake) TestRepo() string {
	return filepath.Join(gm.TestRepoStoragePath(), "gitlab-test.git")
}

func (gm *gitalyMake) GitTestRepo() string {
	return filepath.Join(gm.TestRepoStoragePath(), "gitlab-git-test.git")
}

func (gm *gitalyMake) CommandPackages() []string {
	if len(gm.commandPackages) > 0 {
		return gm.commandPackages
	}

	entries, err := ioutil.ReadDir(filepath.Join(gm.SourceDir(), "cmd"))
	if err != nil {
		log.Fatal(err)
	}

	for _, dir := range entries {
		if !dir.IsDir() {
			continue
		}

		gm.commandPackages = append(gm.commandPackages, filepath.Join(gm.Pkg(), "cmd", dir.Name()))
	}

	return gm.commandPackages
}

func (gm *gitalyMake) Commands() []string {
	var out []string
	for _, pkg := range gm.CommandPackages() {
		out = append(out, filepath.Base(pkg))
	}
	return out
}

func (gm *gitalyMake) BuildTime() string {
	if len(gm.buildTime) > 0 {
		return gm.buildTime
	}

	now := time.Now().UTC()
	gm.buildTime = fmt.Sprintf("%d%02d%02d.%02d%02d%02d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
	return gm.buildTime
}

func (gm *gitalyMake) GoLdFlags() string {
	return fmt.Sprintf("-ldflags '-X %s/internal/version.version=%s -X %s/internal/version.buildtime=%s'", gm.Pkg(), gm.Version(), gm.Pkg(), gm.BuildTime())
}

func (gm *gitalyMake) VersionPrefixed() string {
	if len(gm.versionPrefixed) > 0 {
		return gm.versionPrefixed
	}

	cmd := exec.Command("git", "describe")
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		log.Printf("%s: %v", strings.Join(cmd.Args, " "), err)
		gm.versionPrefixed = "unknown"
		return gm.versionPrefixed
	}
	gm.versionPrefixed = strings.TrimSpace(string(out))

	return gm.versionPrefixed
}

func (gm *gitalyMake) Version() string { return strings.TrimPrefix(gm.VersionPrefixed(), "v") }

func (gm *gitalyMake) GoFiles() []string {
	if len(gm.goFiles) > 0 {
		return gm.goFiles
	}

	root := gm.SourceDir() + "/." // Add "/." to traverse symlink

	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() && path != root {
			if path == filepath.Join(root, "ruby") || path == filepath.Join(root, "vendor") {
				return filepath.SkipDir
			}

			if name := info.Name(); name == "testdata" || strings.HasPrefix(name, "_") || strings.HasPrefix(name, ".") {
				return filepath.SkipDir
			}
		}

		if !info.IsDir() && strings.HasSuffix(path, ".go") {
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			gm.goFiles = append(gm.goFiles, rel)
		}

		return nil
	})

	sort.Strings(gm.goFiles)

	return gm.goFiles
}

func (gm *gitalyMake) AllPackages() []string {
	pkgMap := make(map[string]struct{})
	for _, f := range gm.GoFiles() {
		pkgMap[filepath.Dir(filepath.Join(gm.Pkg(), f))] = struct{}{}
	}

	var pkgs []string
	for k := range pkgMap {
		pkgs = append(pkgs, k)
	}

	sort.Strings(pkgs)

	return pkgs
}

var templateText = `
# _build/Makefile
#
# This is an auto-generated Makefile. Do not edit. Do not invoke
# directly, use ../Makefile instead. This file is generated using
# makegen.go.
#

# These variables may be overriden at runtime by top-level make
PREFIX ?= /usr/local
INSTALL_DEST_DIR := $(DESTDIR)$(PREFIX)/bin/
BUNDLE_FLAGS ?= --deployment
ASSEMBLY_ROOT ?= {{ .BuildDir }}/assembly
BUILD_TAGS := tracer_static tracer_static_jaeger

unexport GOROOT
unexport GOBIN

.NOTPARALLEL:

.PHONY: all
all: build

.PHONY: build
build: ../.ruby-bundle
	go install {{ .GoLdFlags }} -tags "$(BUILD_TAGS)" {{ join .CommandPackages " " }}

# This file is used by Omnibus and CNG to skip the "bundle install"
# step. Both Omnibus and CNG assume it is in the Gitaly root, not in
# _build. Hence the '../' in front.
../.ruby-bundle:  {{ .GitalyRubyDir }}/Gemfile.lock  {{ .GitalyRubyDir }}/Gemfile
	cd  {{ .GitalyRubyDir }} && bundle config # for debugging
	cd  {{ .GitalyRubyDir }} && bundle install $(BUNDLE_FLAGS)
	cd  {{ .GitalyRubyDir }} && bundle show gitaly-proto # sanity check
	touch $@

.PHONY: install
install: build
	mkdir -p $(INSTALL_DEST_DIR)
	cd bin && install {{ join .Commands " " }} $(INSTALL_DEST_DIR)

.PHONY: force-ruby-bundle
force-ruby-bundle:
	rm -f ../.ruby-bundle

# Assembles all runtime components into a directory
# Used by the GDK: run 'make assemble ASSEMBLY_ROOT=.../gitaly'
.PHONY: assemble
assemble: force-ruby-bundle build assemble-internal

# assemble-internal does not force 'bundle install' to run again
.PHONY: assemble-internal
assemble-internal: assemble-ruby assemble-go

.PHONY: assemble-go
assemble-go: build
	rm -rf $(ASSEMBLY_ROOT)/bin
	mkdir -p $(ASSEMBLY_ROOT)/bin
	cd bin && install {{ join .Commands " " }} $(ASSEMBLY_ROOT)/bin

.PHONY: assemble-ruby
assemble-ruby:
	rm -rf $(ASSEMBLY_ROOT)/ruby
	mkdir -p $(ASSEMBLY_ROOT)
	rm -rf {{ .GitalyRubyDir }}/tmp {{ .GitlabShellDir }}/tmp 
	cp -r  {{ .GitalyRubyDir }} $(ASSEMBLY_ROOT)/ruby
	rm -rf $(ASSEMBLY_ROOT)/ruby/spec $(ASSEMBLY_ROOT)/{{ .GitlabShellRelDir }}/spec $(ASSEMBLY_ROOT)/{{ .GitlabShellRelDir }}/gitlab-shell.log

binaries: assemble
	@if [ $$(uname -m) != 'x86_64' ]; then echo Incorrect architecture for build: $(uname -m); exit 1; fi
	@cd $(ASSEMBLY_ROOT) && shasum -a 256 bin/* | tee checksums.sha256.txt

{{ .TestRepo }}:
	git clone --bare --quiet https://gitlab.com/gitlab-org/gitlab-test.git $@
	# Git notes aren't fetched by default with git clone
	git -C $@ fetch origin refs/notes/*:refs/notes/*
	rm -rf $@/refs
	mkdir -p $@/refs/heads $@/refs/tags
	cp {{ .SourceDir }}/_support/gitlab-test.git-packed-refs $@/packed-refs
	git -C $@ fsck --no-progress

{{ .GitTestRepo }}:
	git clone --bare --quiet https://gitlab.com/gitlab-org/gitlab-git-test.git $@
	rm -rf $@/refs
	mkdir -p $@/refs/heads $@/refs/tags
	cp {{ .SourceDir }}/_support/gitlab-git-test.git-packed-refs $@/packed-refs
	git -C $@ fsck --no-progress

.PHONY: prepare-tests
prepare-tests: {{ .TestRepo }} {{ .GitTestRepo }} ../.ruby-bundle

.PHONY: test
test: test-go rspec rspec-gitlab-shell

.PHONY: test-go
test-go: prepare-tests
	@go test -tags "$(BUILD_TAGS)" -count=1 {{ join .AllPackages " " }} # count=1 bypasses go 1.10 test caching

.PHONY: race-go
race-go: prepare-tests
	@go test -tags "$(BUILD_TAGS)" -race {{ join .AllPackages " " }}

.PHONY: rspec
rspec: assemble-go prepare-tests
	cd  {{ .GitalyRubyDir }} && bundle exec rspec

.PHONY: rspec-gitlab-shell
rspec-gitlab-shell: {{ .GitlabShellDir }}/config.yml assemble-go prepare-tests
	# rspec in {{ .GitlabShellRelDir }}
	@cd  {{ .GitalyRubyDir }} && bundle exec bin/ruby-cd {{ .GitlabShellDir }} rspec

{{ .GitlabShellDir }}/config.yml: {{ .GitlabShellDir }}/config.yml.example
	cp $< $@

.PHONY: verify
verify: lint check-formatting staticcheck govendor-status notice-up-to-date govendor-tagged rubocop

.PHONY: lint
lint: {{ .GoLint }}
	# golint
	@cd {{ .SourceDir }} && go run _support/lint.go

{{ .GoLint }}:
	go get golang.org/x/lint/golint

.PHONY: check-formatting
check-formatting: {{ .GoImports }}
	# goimports
	@cd {{ .SourceDir }} && goimports -e -l {{ join .GoFiles " " }} | awk '{ print } END { if(NR>0) { print "Formatting error, run make format"; exit(1) } }'

{{ .GoImports }}:
	go get golang.org/x/tools/cmd/goimports

.PHONY: format
format: {{ .GoImports }}
	# In addition to fixing imports, goimports also formats your code in the same style as gofmt
	# so it can be used as a replacement.
	@cd {{ .SourceDir }} && goimports -w -l {{ join .GoFiles " " }}

.PHONY: staticcheck
staticcheck: {{ .StaticCheck }}
	# staticcheck
	@cd {{ .SourceDir }} && {{ .StaticCheck }} {{ join .AllPackages " " }}

# Install staticcheck
{{ .StaticCheck }}:
	go get honnef.co/go/tools/cmd/staticcheck

.PHONY: govendor-status
govendor-status: {{ .GoVendor }}
	# govendor status
	@cd {{ .SourceDir }} && govendor status

{{ .GoVendor }}:
	go get github.com/kardianos/govendor

.PHONY: notice-up-to-date
notice-up-to-date: {{ .GoVendor }} clean-ruby-vendor-go
	# notice-up-to-date
	@(cd {{ .SourceDir }} && govendor license -template _support/notice.template | cmp - NOTICE) || (echo >&2 "NOTICE requires update: 'make notice'" && false)

.PHONY: notice 
notice: {{ .GoVendor }} clean-ruby-vendor-go
	cd {{ .SourceDir }} && govendor license -template _support/notice.template -o NOTICE

.PHONY: clean-ruby-vendor-go 
clean-ruby-vendor-go:
	cd {{ .SourceDir }} && mkdir -p ruby/vendor && find ruby/vendor -type f -name '*.go' -delete

.PHONY: govendor-tagged
govendor-tagged: {{ .GoVendor }}
	# govendor-tagged
	@cd {{ .SourceDir }} && _support/gitaly-proto-tagged

.PHONY: rubocop
rubocop: ../.ruby-bundle
	cd  {{ .GitalyRubyDir }} && bundle exec rubocop --parallel

.PHONY: cover
cover: prepare-tests {{ .GoCovMerge }}
	@echo "NOTE: make cover does not exit 1 on failure, don't use it to check for tests success!"
	mkdir -p "{{ .CoverageDir }}"
	rm -f {{ .CoverageDir }}/*.out "{{ .CoverageDir }}/all.merged" "{{ .CoverageDir }}/all.html"
	@cd {{ .SourceDir }} && go run _support/test-cover-parallel.go {{ .CoverageDir }} {{ join .AllPackages " " }}
	{{ .GoCovMerge }} {{ .CoverageDir }}/*.out > "{{ .CoverageDir }}/all.merged"
	go tool cover -html  "{{ .CoverageDir }}/all.merged" -o "{{ .CoverageDir }}/all.html"
	@echo ""
	@echo "=====> Total test coverage: <====="
	@echo ""
	@go tool cover -func "{{ .CoverageDir }}/all.merged"

{{ .GoCovMerge }}:
	go get github.com/wadey/gocovmerge

.PHONY: docker
docker:
	rm -rf docker/
	mkdir -p docker/bin/
	rm -rf  {{ .GitalyRubyDir }}/tmp
	cp -r  {{ .GitalyRubyDir }} docker/ruby
	rm -rf docker/ruby/vendor/bundle
{{ $pkg := .Pkg }}
{{ $goLdFlags := .GoLdFlags }}
{{ range $cmd := .Commands }}
	GOOS=linux GOARCH=amd64 go build {{ $goLdFlags }} -o "docker/bin/{{ $cmd }}" {{ $pkg }}/cmd/{{ $cmd }}
{{ end }}
	cp {{ .SourceDir }}/Dockerfile docker/
	docker build -t gitlab/gitaly:{{ .VersionPrefixed }} -t gitlab/gitaly:latest docker/
`
