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
	"runtime"
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

func (gm *gitalyMake) Pkg() string         { return "gitlab.com/gitlab-org/gitaly" }
func (gm *gitalyMake) GoImports() string   { return "bin/goimports" }
func (gm *gitalyMake) GoCovMerge() string  { return "bin/gocovmerge" }
func (gm *gitalyMake) GoLint() string      { return "bin/golint" }
func (gm *gitalyMake) GoVendor() string    { return "bin/govendor" }
func (gm *gitalyMake) StaticCheck() string { return filepath.Join(gm.BuildDir(), "bin/staticcheck") }
func (gm *gitalyMake) ProtoC() string      { return filepath.Join(gm.BuildDir(), "protoc/bin/protoc") }
func (gm *gitalyMake) ProtoCGenGo() string { return filepath.Join(gm.BuildDir(), "bin/protoc-gen-go") }
func (gm *gitalyMake) ProtoCGenGitaly() string {
	return filepath.Join(gm.BuildDir(), "bin/protoc-gen-gitaly")
}
func (gm *gitalyMake) GrpcToolsRuby() string {
	return filepath.Join(gm.BuildDir(), "bin/grpc_tools_ruby_protoc")
}
func (gm *gitalyMake) CoverageDir() string       { return filepath.Join(gm.BuildDir(), "cover") }
func (gm *gitalyMake) GitalyRubyDir() string     { return filepath.Join(gm.SourceDir(), "ruby") }
func (gm *gitalyMake) GitlabShellRelDir() string { return "ruby/gitlab-shell" }
func (gm *gitalyMake) GitlabShellDir() string {
	return filepath.Join(gm.SourceDir(), gm.GitlabShellRelDir())
}

func (gm *gitalyMake) GopathSourceDir() string {
	return filepath.Join(gm.BuildDir(), "src", gm.Pkg())
}

func (gm *gitalyMake) Git2GoVendorDir() string {
	return filepath.Join(gm.BuildDir(), "../vendor/github.com/libgit2/git2go/vendor")
}

func (gm *gitalyMake) LibGit2Version() string {
	return filepath.Join("0.27.8")
}

func (gm *gitalyMake) LibGit2SHA() string {
	return filepath.Join("8313873d49dc01e8b880ec334d7430ae67496a89aaa8c6e7bbd3affb47a00c76")
}

func (gm *gitalyMake) SourceDir() string {
	return os.Getenv("SOURCE_DIR")
}

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

func (gm *gitalyMake) GitalyRemotePackage() string {
	return filepath.Join(gm.Pkg(), "cmd", "gitaly-remote")
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
		//Do not build gitaly-remote by default
		if dir.Name() == "gitaly-remote" {
			continue
		}
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

func (gm *gitalyMake) VersionFromFile() string {
	data, err := ioutil.ReadFile("../VERSION")
	if err != nil {
		log.Printf("error obtaining version from file: %v", err)
		return ""
	}

	return fmt.Sprintf("v%s", strings.TrimSpace(string(data)))
}

func (gm *gitalyMake) VersionFromGit() string {
	cmd := exec.Command("git", "describe")
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		log.Printf("error obtaining version from git: %s: %v", strings.Join(cmd.Args, " "), err)
		return ""
	}

	return strings.TrimSpace(string(out))
}

func (gm *gitalyMake) VersionPrefixed() string {
	if len(gm.versionPrefixed) > 0 {
		return gm.versionPrefixed
	}

	version := gm.VersionFromGit()
	if version == "" {
		log.Printf("Attempting to get the version from file")
		version = gm.VersionFromFile()
	}

	if version == "" {
		version = "unknown"
	}

	gm.versionPrefixed = version
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
			switch path {
			case filepath.Join(root, "ruby"):
				return filepath.SkipDir
			case filepath.Join(root, "vendor"):
				return filepath.SkipDir
			case filepath.Join(root, "proto/go"):
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
		//Do not build gitaly-remote by default
		if k == "gitlab.com/gitlab-org/gitaly/cmd/gitaly-remote" {
			continue
		}
		pkgs = append(pkgs, k)
	}

	sort.Strings(pkgs)

	return pkgs
}

type protoDownloadInfo struct {
	url    string
	sha256 string
}

var protoCDownload = map[string]protoDownloadInfo{
	"darwin/amd64": protoDownloadInfo{
		url:    "https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protoc-3.6.1-osx-x86_64.zip",
		sha256: "0decc6ce5beed07f8c20361ddeb5ac7666f09cf34572cca530e16814093f9c0c",
	},
	"linux/amd64": protoDownloadInfo{
		url:    "https://github.com/protocolbuffers/protobuf/releases/download/v3.6.1/protoc-3.6.1-linux-x86_64.zip",
		sha256: "6003de742ea3fcf703cfec1cd4a3380fd143081a2eb0e559065563496af27807",
	},
}

func (gm *gitalyMake) ProtoCURL() string {
	return protoCDownload[runtime.GOOS+"/"+runtime.GOARCH].url
}

func (gm *gitalyMake) ProtoCSHA256() string {
	return protoCDownload[runtime.GOOS+"/"+runtime.GOARCH].sha256
}

var templateText = `
# _build/Makefile
#
# This is an auto-generated Makefile. Do not edit. Do not invoke
# directly, use ../Makefile instead. This file is generated using
# makegen.go.
#

# These variables may be overridden at runtime by top-level make
PREFIX ?= /usr/local
INSTALL_DEST_DIR := $(DESTDIR)$(PREFIX)/bin/
BUNDLE_FLAGS ?= --deployment
ASSEMBLY_ROOT ?= {{ .BuildDir }}/assembly
BUILD_TAGS := tracer_static tracer_static_jaeger

unexport GOROOT
export GOBIN = {{ .BuildDir }}/bin
unexport GOPATH
export GO111MODULE=on
export GOPROXY ?= https://proxy.golang.org

.NOTPARALLEL:

.PHONY: all
all: build

{{ .Git2GoVendorDir }}/.ok:
	rm -rf {{ .Git2GoVendorDir }} 
	mkdir -p {{ .Git2GoVendorDir }}

	cd {{ .Git2GoVendorDir }} && curl -L -o libgit2.tar.gz https://github.com/libgit2/libgit2/archive/v{{ .LibGit2Version }}.tar.gz
	cd {{ .Git2GoVendorDir }} && echo '{{ .LibGit2SHA }}  libgit2.tar.gz' | shasum -a256 -c -
	cd {{ .Git2GoVendorDir }} && tar -xvf libgit2.tar.gz
	cd {{ .Git2GoVendorDir }} && mv libgit2-{{ .LibGit2Version }} libgit2

	mkdir -p {{ .Git2GoVendorDir }}/libgit2/build
	mkdir -p {{ .Git2GoVendorDir }}/libgit2/install/lib
	cd {{ .Git2GoVendorDir }}/libgit2/build && cmake -DTHREADSAFE=ON -DBUILD_CLAR=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_C_FLAGS=-fPIC -DCMAKE_BUILD_TYPE="RelWithDebInfo" -DCMAKE_INSTALL_PREFIX=../install ..
	cd {{ .Git2GoVendorDir }}/libgit2/build && cmake --build .

	touch $@

.PHONY: build-gitaly-remote
build-gitaly-remote: {{ .Git2GoVendorDir }}/.ok
	cd {{ .SourceDir }} && go install {{ .GoLdFlags }} -tags "$(BUILD_TAGS) static" {{ .GitalyRemotePackage }}

.PHONY: test-gitaly-remote
test-gitaly-remote: prepare-tests {{ .Git2GoVendorDir }}/.ok
	@go test -tags "$(BUILD_TAGS) static" -count=1 {{ .GitalyRemotePackage }}

.PHONY: build
build: ../.ruby-bundle
	# go install
	@cd {{ .SourceDir }} && go install {{ .GoLdFlags }} -tags "$(BUILD_TAGS)" {{ join .CommandPackages " " }}

# This file is used by Omnibus and CNG to skip the "bundle install"
# step. Both Omnibus and CNG assume it is in the Gitaly root, not in
# _build. Hence the '../' in front.
../.ruby-bundle:  {{ .GitalyRubyDir }}/Gemfile.lock  {{ .GitalyRubyDir }}/Gemfile
	cd  {{ .GitalyRubyDir }} && bundle config # for debugging
	cd  {{ .GitalyRubyDir }} && bundle install $(BUNDLE_FLAGS)
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
	mkdir -p $(ASSEMBLY_ROOT)
	rm -rf {{ .GitalyRubyDir }}/tmp {{ .GitlabShellDir }}/tmp
	mkdir -p $(ASSEMBLY_ROOT)/ruby/
	rsync -a --delete  {{ .GitalyRubyDir }}/ $(ASSEMBLY_ROOT)/ruby/
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
prepare-tests: {{ .GitlabShellDir }}/config.yml {{ .TestRepo }} {{ .GitTestRepo }} ../.ruby-bundle

{{ .GitlabShellDir }}/config.yml: {{ .GitlabShellDir }}/config.yml.example
	cp $< $@

.PHONY: test
test: test-go rspec rspec-gitlab-shell

.PHONY: test-go
test-go: prepare-tests
	@cd {{ .SourceDir }} && go test -tags "$(BUILD_TAGS)" -count=1 {{ join .AllPackages " " }} # count=1 bypasses go 1.10 test caching

.PHONY: test-with-proxies
test-with-proxies: prepare-tests
	@cd {{ .SourceDir }} &&\
		go test -tags "$(BUILD_TAGS)" -count=1  -exec {{ .SourceDir }}/_support/bad-proxies {{ .Pkg }}/internal/rubyserver/

.PHONY: race-go
race-go: prepare-tests
	@cd {{ .SourceDir }} && go test -tags "$(BUILD_TAGS)" -race {{ join .AllPackages " " }}

.PHONY: rspec
rspec: assemble-go prepare-tests
	cd  {{ .GitalyRubyDir }} && bundle exec rspec

.PHONY: rspec-gitlab-shell
rspec-gitlab-shell: {{ .GitlabShellDir }}/config.yml assemble-go prepare-tests
	# rspec in {{ .GitlabShellRelDir }}
	@cd  {{ .GitalyRubyDir }} && bundle exec bin/ruby-cd {{ .GitlabShellDir }} rspec

.PHONY: verify
verify: check-mod-tidy lint check-formatting staticcheck notice-up-to-date check-proto rubocop

.PHONY: check-mod-tidy
check-mod-tidy:
	# check-mod-tidy
	@cd {{ .SourceDir }} && _support/check-mod-tidy

.PHONY: lint
lint: {{ .GoLint }}
	# golint
	@cd {{ .SourceDir }} && go run _support/lint.go

{{ .GoLint }}:
	go get golang.org/x/lint/golint@959b441ac422379a43da2230f62be024250818b0

.PHONY: check-formatting
check-formatting: {{ .GoImports }}
	# goimports
	@cd {{ .SourceDir }} && goimports -e -l {{ join .GoFiles " " }} | awk '{ print } END { if(NR>0) { print "Formatting error, run make format"; exit(1) } }'

{{ .GoImports }}:
	go get golang.org/x/tools/cmd/goimports@2538eef75904eff384a2551359968e40c207d9d2

.PHONY: format
format: {{ .GoImports }}
	# In addition to fixing imports, goimports also formats your code in the same style as gofmt
	# so it can be used as a replacement.
	@cd {{ .SourceDir }} && goimports -w -l {{ join .GoFiles " " }}

.PHONY: staticcheck
staticcheck: {{ .StaticCheck }}
	# staticcheck
	@cd {{ .SourceDir }} && {{ .StaticCheck }} -tags "$(BUILD_TAGS) static" {{ join .AllPackages " " }}

# Install staticcheck
{{ .StaticCheck }}:
	go get honnef.co/go/tools/cmd/staticcheck@95959eaf5e3c41c66151dcfd91779616b84077a8

{{ .GoVendor }}:
	go get github.com/kardianos/govendor@e07957427183a9892f35634ffc9ea48dedc6bbb4

.PHONY: notice-up-to-date
notice-up-to-date: notice-tmp
	# notice-up-to-date
	@(cmp {{ .BuildDir }}/NOTICE {{ .SourceDir }}/NOTICE) || (echo >&2 "NOTICE requires update: 'make notice'" && false)

.PHONY: notice
notice: notice-tmp
	mv {{ .BuildDir }}/NOTICE {{ .SourceDir }}/NOTICE

.PHONY: notice-tmp
notice-tmp: {{ .GoVendor }} clean-ruby-vendor-go
	rm -rf {{ .SourceDir }}/vendor
	cd {{ .SourceDir }} && go mod vendor
	cd {{ .GopathSourceDir }} && env GOPATH={{ .BuildDir }} GO111MODULE=off govendor license -template _support/notice.template -o {{ .BuildDir }}/NOTICE

.PHONY: clean-ruby-vendor-go
clean-ruby-vendor-go:
	cd {{ .SourceDir }} && mkdir -p ruby/vendor && find ruby/vendor -type f -name '*.go' -delete

.PHONY: check-proto
check-proto: proto no-changes
	# checking gitaly-proto revision
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
	@cd {{ .SourceDir }} && go tool cover -html  "{{ .CoverageDir }}/all.merged" -o "{{ .CoverageDir }}/all.html"
	@echo ""
	@echo "=====> Total test coverage: <====="
	@echo ""
	@@cd {{ .SourceDir }} && go tool cover -func "{{ .CoverageDir }}/all.merged"

{{ .GoCovMerge }}:
	go get github.com/wadey/gocovmerge@b5bfa59ec0adc420475f97f89b58045c721d761c

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
	GOOS=linux GOARCH=amd64 go build -tags "$(BUILD_TAGS)" {{ $goLdFlags }} -o "docker/bin/{{ $cmd }}" {{ $pkg }}/cmd/{{ $cmd }}
{{ end }}
	cp {{ .SourceDir }}/Dockerfile docker/
	docker build -t gitlab/gitaly:{{ .VersionPrefixed }} -t gitlab/gitaly:latest docker/

.PHONY: proto
proto: {{ .ProtoC }} {{ .ProtoCGenGo }} {{ .ProtoCGenGitaly }} {{ .GrpcToolsRuby }}
	mkdir -p {{ .SourceDir }}/proto/go/gitalypb
	rm -rf {{ .SourceDir }}/proto/go/gitalypb/*.pb.go
	cd {{ .SourceDir }} && {{ .ProtoC }} --gitaly_out=proto_dir=./proto,gitalypb_dir=./proto/go/gitalypb:. --go_out=paths=source_relative,plugins=grpc:./proto/go/gitalypb -I./proto ./proto/*.proto
	cd {{ .SourceDir }} && _support/generate-proto-ruby

{{ .ProtoC }}: {{ .BuildDir }}/protoc.zip
	mkdir -p {{ .BuildDir }}/protoc
	cd {{ .BuildDir }}/protoc && unzip {{ .BuildDir }}/protoc.zip
	touch $@

{{ .BuildDir }}/protoc.zip:
	curl -o $@.tmp --silent -L {{ .ProtoCURL }}
	printf '{{ .ProtoCSHA256 }}  $@.tmp' | shasum -a256 -c -
	mv $@.tmp $@

{{ .ProtoCGenGo }}:
	go get github.com/golang/protobuf/protoc-gen-go@v1.3.2

{{ .ProtoCGenGitaly }}:
	git clone --quiet -b v1.39.0 https://gitlab.com/gitlab-org/gitaly-proto.git {{ .BuildDir }}/gitaly-proto
	cd {{ .BuildDir }}/gitaly-proto/go/internal/cmd/protoc-gen-gitaly && go build -o $@

{{ .GrpcToolsRuby }}:
	gem install --bindir {{ .BuildDir }}/bin -v 1.0.1 grpc-tools

no-changes:
	# looking for changed files
	@cd {{ .SourceDir }} && git status --porcelain | awk '{ print } END { if (NR > 0) { exit 1 } }'

smoke-test: all rspec
	@cd {{ .SourceDir }} && go test ./internal/rubyserver
`
