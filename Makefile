# Makefile for Gitaly

# You can override options by creating a "config.mak" file in Gitaly's root
# directory.
-include config.mak

# Call `make V=1` in order to print commands verbosely.
ifeq ($(V),1)
    Q =
else
    Q = @
endif

SHELL = /usr/bin/env bash -eo pipefail

# Host information
OS   := $(shell uname)
ARCH := $(shell uname -m)

# Directories
SOURCE_DIR       := $(abspath $(dir $(lastword ${MAKEFILE_LIST})))
BUILD_DIR        := ${SOURCE_DIR}/_build
COVERAGE_DIR     := ${BUILD_DIR}/cover
DEPENDENCY_DIR   := ${BUILD_DIR}/deps
GITALY_RUBY_DIR  := ${SOURCE_DIR}/ruby

# These variables may be overridden at runtime by top-level make
PREFIX           ?= /usr/local
prefix           ?= ${PREFIX}
exec_prefix      ?= ${prefix}
bindir           ?= ${exec_prefix}/bin
INSTALL_DEST_DIR := ${DESTDIR}${bindir}
ASSEMBLY_ROOT    ?= ${BUILD_DIR}/assembly
GIT_PREFIX       ?= ${GIT_INSTALL_DIR}

# Tools
GIT               := $(shell which git)
PROTOC            := ${BUILD_DIR}/protoc/bin/protoc
GOIMPORTS         := ${BUILD_DIR}/tools/goimports
GITALYFMT         := ${BUILD_DIR}/tools/gitalyfmt
GOLANGCI_LINT     := ${BUILD_DIR}/tools/golangci-lint
GO_LICENSES       := ${BUILD_DIR}/tools/go-licenses
PROTOC_GEN_GO     := ${BUILD_DIR}/tools/protoc-gen-go
PROTOC_GEN_GITALY := ${BUILD_DIR}/tools/protoc-gen-gitaly
GO_JUNIT_REPORT   := ${BUILD_DIR}/tools/go-junit-report
GOCOVER_COBERTURA := ${BUILD_DIR}/tools/gocover-cobertura

# Tool options
GOLANGCI_LINT_OPTIONS ?=
GOLANGCI_LINT_CONFIG  ?= ${SOURCE_DIR}/.golangci.yml

# Build information
BUNDLE_FLAGS    ?= $(shell test -f ${SOURCE_DIR}/../.gdk-install-root && echo --no-deployment || echo --deployment)
GITALY_PACKAGE  := gitlab.com/gitlab-org/gitaly
BUILD_TIME      := $(shell date +"%Y%m%d.%H%M%S")
GITALY_VERSION  := $(shell git describe --match v* 2>/dev/null | sed 's/^v//' || cat ${SOURCE_DIR}/VERSION 2>/dev/null || echo unknown)
GO_LDFLAGS      := -ldflags '-X ${GITALY_PACKAGE}/internal/version.version=${GITALY_VERSION} -X ${GITALY_PACKAGE}/internal/version.buildtime=${BUILD_TIME}'
GO_BUILD_TAGS   := tracer_static,tracer_static_jaeger,continuous_profiler_stackdriver,static,system_libgit2

# Dependency versions
GOLANGCI_LINT_VERSION     ?= 1.33.0
PROTOC_VERSION            ?= 3.12.4
PROTOC_GEN_GO_VERSION     ?= 1.3.2
GIT_VERSION               ?= v2.29.0
GIT2GO_VERSION            ?= v30
LIBGIT2_VERSION       	  ?= v1.0.1
GOCOVER_COBERTURA_VERSION ?= aaee18c8195c3f2d90e5ef80ca918d265463842a

# Dependency downloads
ifeq (${OS},Darwin)
    PROTOC_URL            ?= https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-osx-x86_64.zip
    PROTOC_HASH           ?= 210227683a5db4a9348cd7545101d006c5829b9e823f3f067ac8539cb387519e
else ifeq (${OS},Linux)
    PROTOC_URL            ?= https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip
    PROTOC_HASH           ?= d0d4c7a3c08d3ea9a20f94eaface12f5d46d7b023fe2057e834a4181c9e93ff3
endif

# Git target
GIT_REPO_URL      ?= https://gitlab.com/gitlab-org/gitlab-git.git
GIT_BINARIES_URL  ?= https://gitlab.com/gitlab-org/gitlab-git/-/jobs/artifacts/${GIT_VERSION}/raw/git_full_bins.tgz?job=build
GIT_BINARIES_HASH ?= 8c88d2adb46d1d07f258904b227c93b8a5a4942ac32a1e54057f215401332141
GIT_INSTALL_DIR   := ${DEPENDENCY_DIR}/git/install
GIT_SOURCE_DIR    := ${DEPENDENCY_DIR}/git/source

ifeq (${GIT_BUILD_OPTIONS},)
    # activate developer checks
    GIT_BUILD_OPTIONS += DEVELOPER=1
    # but don't cause warnings to fail the build
    GIT_BUILD_OPTIONS += DEVOPTS=no-error
    GIT_BUILD_OPTIONS += USE_LIBPCRE2=YesPlease
    GIT_BUILD_OPTIONS += NO_PERL=YesPlease
    GIT_BUILD_OPTIONS += NO_EXPAT=YesPlease
    GIT_BUILD_OPTIONS += NO_TCLTK=YesPlease
    # fix compilation on musl libc
    GIT_BUILD_OPTIONS += NO_REGEX=YesPlease
    GIT_BUILD_OPTIONS += NO_GETTEXT=YesPlease
    GIT_BUILD_OPTIONS += NO_PYTHON=YesPlease
    GIT_BUILD_OPTIONS += NO_INSTALL_HARDLINKS=YesPlease
endif

# libgit2 target
LIBGIT2_REPO_URL    ?= https://gitlab.com/libgit2/libgit2
LIBGIT2_SOURCE_DIR  ?= ${DEPENDENCY_DIR}/libgit2/source
LIBGIT2_BUILD_DIR   ?= ${DEPENDENCY_DIR}/libgit2/build
LIBGIT2_INSTALL_DIR ?= ${DEPENDENCY_DIR}/libgit2/install

ifeq (${LIBGIT2_BUILD_OPTIONS},)
    LIBGIT2_BUILD_OPTIONS += -DTHREADSAFE=ON
    LIBGIT2_BUILD_OPTIONS += -DBUILD_CLAR=OFF
    LIBGIT2_BUILD_OPTIONS += -DBUILD_SHARED_LIBS=OFF
    LIBGIT2_BUILD_OPTIONS += -DCMAKE_C_FLAGS=-fPIC
    LIBGIT2_BUILD_OPTIONS += -DCMAKE_BUILD_TYPE=Release
    LIBGIT2_BUILD_OPTIONS += -DCMAKE_INSTALL_PREFIX=${LIBGIT2_INSTALL_DIR}
    LIBGIT2_BUILD_OPTIONS += -DCMAKE_INSTALL_LIBDIR=lib
    LIBGIT2_BUILD_OPTIONS += -DENABLE_TRACE=OFF
    LIBGIT2_BUILD_OPTIONS += -DUSE_SSH=OFF
    LIBGIT2_BUILD_OPTIONS += -DUSE_HTTPS=OFF
    LIBGIT2_BUILD_OPTIONS += -DUSE_ICONV=OFF
    LIBGIT2_BUILD_OPTIONS += -DUSE_NTLMCLIENT=OFF
    LIBGIT2_BUILD_OPTIONS += -DUSE_BUNDLED_ZLIB=ON
    LIBGIT2_BUILD_OPTIONS += -DUSE_HTTP_PARSER=builtin
    LIBGIT2_BUILD_OPTIONS += -DREGEX_BACKEND=builtin
endif

# These variables control test options and artifacts
TEST_PACKAGES    ?= $(call find_go_packages)
TEST_OPTIONS     ?=
TEST_REPORT_DIR  ?= ${BUILD_DIR}/reports
TEST_OUTPUT_NAME ?= go-${GO_VERSION}-git-${GIT_VERSION}
TEST_OUTPUT      ?= ${TEST_REPORT_DIR}/go-tests-output-${TEST_OUTPUT_NAME}.txt
TEST_REPORT      ?= ${TEST_REPORT_DIR}/go-tests-report-${TEST_OUTPUT_NAME}.xml
TEST_EXIT        ?= ${TEST_REPORT_DIR}/go-tests-exit-${TEST_OUTPUT_NAME}.txt
TEST_REPO_DIR    := ${BUILD_DIR}/testrepos
TEST_REPO        := ${TEST_REPO_DIR}/gitlab-test.git
TEST_REPO_GIT    := ${TEST_REPO_DIR}/gitlab-git-test.git

# uniq is a helper function to filter out any duplicate values in the single
# parameter it accepts.
#
# Credits go to https://stackoverflow.com/questions/16144115/makefile-remove-duplicate-words-without-sorting
uniq = $(if $(1),$(firstword $(1)) $(call uniq,$(filter-out $(firstword $(1)),$(1))))

# Find all commands.
find_commands         = $(notdir $(shell find ${SOURCE_DIR}/cmd -mindepth 1 -maxdepth 1 -type d -print))
# Find all command binaries.
find_command_binaries = $(addprefix ${BUILD_DIR}/bin/, $(call find_commands))

# Find all Go source files.
find_go_sources  = $(shell find ${SOURCE_DIR} -type d \( -name ruby -o -name vendor -o -name testdata -o -name '_*' -o -path '*/proto/go' \) -prune -o -type f -name '*.go' -not -name '*.pb.go' -print | sort -u)
# Find all Go packages.
find_go_packages = $(call uniq,$(dir $(call find_go_sources)))

# run_go_tests will execute Go tests with all required parameters. Its
# behaviour can be modified via the following variables:
#
# GO_BUILD_TAGS: tags used to build the executables
# TEST_OPTIONS: any additional options
# TEST_PACKAGES: packages which shall be tested
run_go_tests = PATH='${SOURCE_DIR}/internal/testhelper/testdata/home/bin:${PATH}' \
    go test -v -count=1 -tags '${GO_BUILD_TAGS}' ${TEST_OPTIONS} ${TEST_PACKAGES}

unexport GOROOT
export GOBIN                      = ${BUILD_DIR}/bin
export GOCACHE                   ?= ${BUILD_DIR}/cache
export GOPROXY                   ?= https://proxy.golang.org
export PATH                      := ${BUILD_DIR}/bin:${PATH}
export PKG_CONFIG_PATH           := ${LIBGIT2_INSTALL_DIR}/lib/pkgconfig
export GITALY_TESTING_GIT_BINARY ?= ${GIT_INSTALL_DIR}/bin/git
# Allow the linker flag -D_THREAD_SAFE as libgit2 is compiled with it on FreeBSD
export CGO_LDFLAGS_ALLOW          = -D_THREAD_SAFE

.NOTPARALLEL:

.PHONY: all
all: INSTALL_DEST_DIR = ${SOURCE_DIR}
all: install

.PHONY: build
build: ${SOURCE_DIR}/.ruby-bundle libgit2
	go install ${GO_LDFLAGS} -tags "${GO_BUILD_TAGS}" $(addprefix ${GITALY_PACKAGE}/cmd/, $(call find_commands))

.PHONY: install
install: build
	${Q}mkdir -p ${INSTALL_DEST_DIR}
	install $(call find_command_binaries) ${INSTALL_DEST_DIR}

.PHONY: force-ruby-bundle
force-ruby-bundle:
	${Q}rm -f ${SOURCE_DIR}/.ruby-bundle

# Assembles all runtime components into a directory
# Used by the GDK: run 'make assemble ASSEMBLY_ROOT=.../gitaly'
.PHONY: assemble
assemble: force-ruby-bundle assemble-internal

# assemble-internal does not force 'bundle install' to run again
.PHONY: assemble-internal
assemble-internal: assemble-ruby assemble-go

.PHONY: assemble-go
assemble-go: build
	${Q}rm -rf ${ASSEMBLY_ROOT}/bin
	${Q}mkdir -p ${ASSEMBLY_ROOT}/bin
	install $(call find_command_binaries) ${ASSEMBLY_ROOT}/bin

.PHONY: assemble-ruby
assemble-ruby:
	${Q}mkdir -p ${ASSEMBLY_ROOT}
	${Q}rm -rf ${GITALY_RUBY_DIR}/tmp
	${Q}mkdir -p ${ASSEMBLY_ROOT}/ruby/
	rsync -a --delete  ${GITALY_RUBY_DIR}/ ${ASSEMBLY_ROOT}/ruby/
	${Q}rm -rf ${ASSEMBLY_ROOT}/ruby/spec

.PHONY: binaries
binaries: assemble
	${Q}if [ ${ARCH} != 'x86_64' ]; then echo Incorrect architecture for build: ${ARCH}; exit 1; fi
	${Q}cd ${ASSEMBLY_ROOT} && sha256sum bin/* | tee checksums.sha256.txt

.PHONY: prepare-tests
prepare-tests: git libgit2 prepare-test-repos ${SOURCE_DIR}/.ruby-bundle

.PHONY: prepare-test-repos
prepare-test-repos: ${TEST_REPO} ${TEST_REPO_GIT}

.PHONY: test
test: test-go rspec

.PHONY: test-go
test-go: prepare-tests ${GO_JUNIT_REPORT}
	${Q}mkdir -p ${TEST_REPORT_DIR}
	${Q}echo 0 >${TEST_EXIT}
	${Q}$(call run_go_tests) 2>&1 | tee ${TEST_OUTPUT} || echo $$? >${TEST_EXIT}
	${Q}${GO_JUNIT_REPORT} <${TEST_OUTPUT} >${TEST_REPORT}
	${Q}exit `cat ${TEST_EXIT}`

.PHONY: test-with-proxies
test-with-proxies: TEST_OPTIONS  := ${TEST_OPTIONS} -exec ${SOURCE_DIR}/_support/bad-proxies
test-with-proxies: TEST_PACKAGES := ${GITALY_PACKAGE}/internal/gitaly/rubyserver
test-with-proxies: prepare-tests
	${Q}$(call run_go_tests)

.PHONY: test-with-praefect
test-with-praefect: build prepare-tests
	${Q}GITALY_TEST_PRAEFECT_BIN=${BUILD_DIR}/bin/praefect $(call run_go_tests)

.PHONY: test-postgres
test-postgres: GO_BUILD_TAGS := ${GO_BUILD_TAGS},postgres
test-postgres: TEST_PACKAGES := gitlab.com/gitlab-org/gitaly/internal/praefect/...
test-postgres: prepare-tests
	${Q}$(call run_go_tests)

.PHONY: race-go
race-go: TEST_OPTIONS := ${TEST_OPTIONS} -race
race-go: test-go

.PHONY: rspec
rspec: assemble-go prepare-tests
	${Q}cd ${GITALY_RUBY_DIR} && PATH='${SOURCE_DIR}/internal/testhelper/testdata/home/bin:${PATH}' bundle exec rspec

.PHONY: verify
verify: check-mod-tidy check-formatting notice-up-to-date check-proto rubocop

.PHONY: check-mod-tidy
check-mod-tidy:
	${Q}${GIT} diff --quiet --exit-code go.mod go.sum || (echo "error: uncommitted changes in go.mod or go.sum" && exit 1)
	${Q}go mod tidy
	${Q}${GIT} diff --quiet --exit-code go.mod go.sum || (echo "error: uncommitted changes in go.mod or go.sum" && exit 1)

.PHONY: lint
lint: ${GOLANGCI_LINT} libgit2
	${Q}${GOLANGCI_LINT} run --build-tags "${GO_BUILD_TAGS}" --out-format tab --config ${GOLANGCI_LINT_CONFIG} ${GOLANGCI_LINT_OPTIONS}

.PHONY: lint-strict
lint-strict: lint
	${Q}GOLANGCI_LINT_CONFIG=$(SOURCE_DIR)/.golangci-strict.yml $(MAKE) lint

.PHONY: check-formatting
check-formatting: ${GOIMPORTS} ${GITALYFMT}
	${Q}${GOIMPORTS} -l $(call find_go_sources) | awk '{ print } END { if(NR>0) { print "goimports error, run make format"; exit(1) } }'
	${Q}${GITALYFMT} $(call find_go_sources) | awk '{ print } END { if(NR>0) { print "Formatting error, run make format"; exit(1) } }'

.PHONY: format
format: ${GOIMPORTS} ${GITALYFMT}
	${Q}${GOIMPORTS} -w -l $(call find_go_sources)
	${Q}${GITALYFMT} -w $(call find_go_sources)
	${Q}${GOIMPORTS} -w -l $(call find_go_sources)

.PHONY: notice-up-to-date
notice-up-to-date: ${BUILD_DIR}/NOTICE
	${Q}(cmp ${BUILD_DIR}/NOTICE ${SOURCE_DIR}/NOTICE) || (echo >&2 "NOTICE requires update: 'make notice'" && false)

.PHONY: notice
notice: ${SOURCE_DIR}/NOTICE

.PHONY: clean
clean:
	rm -rf ${BUILD_DIR} ${SOURCE_DIR}/internal/testhelper/testdata/data/ ${SOURCE_DIR}/ruby/.bundle/ ${SOURCE_DIR}/ruby/vendor/bundle/ $(addprefix ${SOURCE_DIR}/, $(notdir $(call find_commands)))

.PHONY: clean-ruby-vendor-go
clean-ruby-vendor-go:
	mkdir -p ${SOURCE_DIR}/ruby/vendor && find ${SOURCE_DIR}/ruby/vendor -type f -name '*.go' -delete

.PHONY: check-proto
check-proto: proto no-proto-changes lint-proto

.PHONY: rubocop
rubocop: ${SOURCE_DIR}/.ruby-bundle
	${Q}cd ${GITALY_RUBY_DIR} && bundle exec rubocop --parallel

.PHONY: cover
cover: GO_BUILD_TAGS := ${GO_BUILD_TAGS},postgres
cover: TEST_OPTIONS  := ${TEST_OPTIONS} -coverprofile "${COVERAGE_DIR}/all.merged"
cover: prepare-tests libgit2 ${GOCOVER_COBERTURA}
	${Q}echo "NOTE: make cover does not exit 1 on failure, don't use it to check for tests success!"
	${Q}mkdir -p "${COVERAGE_DIR}"
	${Q}rm -f "${COVERAGE_DIR}/all.merged" "${COVERAGE_DIR}/all.html"
	${Q}$(call run_go_tests)
	${Q}go tool cover -html  "${COVERAGE_DIR}/all.merged" -o "${COVERAGE_DIR}/all.html"
	# sed is used below to convert file paths to repository root relative paths. See https://gitlab.com/gitlab-org/gitlab/-/issues/217664
	${Q}${GOCOVER_COBERTURA} <"${COVERAGE_DIR}/all.merged" | sed 's;filename=\"$(shell go list -m)/;filename=\";g' >"${COVERAGE_DIR}/cobertura.xml"
	${Q}echo ""
	${Q}echo "=====> Total test coverage: <====="
	${Q}echo ""
	${Q}go tool cover -func "${COVERAGE_DIR}/all.merged"

.PHONY: proto
proto: ${PROTOC} ${PROTOC_GEN_GO} ${SOURCE_DIR}/.ruby-bundle
	${Q}mkdir -p ${SOURCE_DIR}/proto/go/gitalypb
	${Q}rm -f ${SOURCE_DIR}/proto/go/gitalypb/*.pb.go
	${PROTOC} --plugin=${PROTOC_GEN_GO} -I ${SOURCE_DIR}/proto --go_out=paths=source_relative,plugins=grpc:${SOURCE_DIR}/proto/go/gitalypb ${SOURCE_DIR}/proto/*.proto
	${SOURCE_DIR}/_support/generate-proto-ruby
	${Q}# this part is related to the generation of sources from testing proto files
	${PROTOC} --plugin=${PROTOC_GEN_GO} -I ${SOURCE_DIR}/internal --go_out=path=source_relative,plugins=grpc:${SOURCE_DIR}/internal ${SOURCE_DIR}/internal/praefect/grpc-proxy/testdata/test.proto
	${PROTOC} --plugin=${PROTOC_GEN_GO} -I ${SOURCE_DIR}/proto -I ${SOURCE_DIR}/internal --go_out=paths=source_relative,plugins=grpc:${SOURCE_DIR}/internal ${SOURCE_DIR}/internal/praefect/mock/mock.proto
	${PROTOC} --plugin=${PROTOC_GEN_GO} -I ${SOURCE_DIR}/proto -I ${SOURCE_DIR}/internal --go_out=paths=source_relative,plugins=grpc:${SOURCE_DIR}/internal ${SOURCE_DIR}/internal/middleware/cache/testdata/stream.proto
	${PROTOC} --plugin=${PROTOC_GEN_GO} -I ${SOURCE_DIR}/proto --go_out=paths=source_relative,plugins=grpc:${SOURCE_DIR}/proto ${SOURCE_DIR}/proto/go/internal/linter/testdata/*.proto

.PHONY: lint-proto
lint-proto: ${PROTOC} ${PROTOC_GEN_GITALY}
	${Q}${PROTOC} --plugin=${PROTOC_GEN_GITALY} -I ${SOURCE_DIR}/proto --gitaly_out=proto_dir=${SOURCE_DIR}/proto,gitalypb_dir=${SOURCE_DIR}/proto/go/gitalypb:${SOURCE_DIR} ${SOURCE_DIR}/proto/*.proto

.PHONY: no-changes
no-changes:
	${Q}${GIT} status --porcelain | awk '{ print } END { if (NR > 0) { exit 1 } }'

.PHONY: no-proto-changes
no-proto-changes: | ${BUILD_DIR}
	${Q}${GIT} diff -- '*.pb.go' 'ruby/proto/gitaly' >${BUILD_DIR}/proto.diff
	${Q}if [ -s ${BUILD_DIR}/proto.diff ]; then echo "There is a difference in generated proto files. Please take a look at ${BUILD_DIR}/proto.diff file." && exit 1; fi

.PHONY: smoke-test
smoke-test: TEST_PACKAGES := ${SOURCE_DIR}/internal/gitaly/rubyserver
smoke-test: all rspec
	$(call run_go_tests)

.PHONY: git
git: ${GIT_INSTALL_DIR}/bin/git

.PHONY: libgit2
libgit2: ${LIBGIT2_INSTALL_DIR}/lib/libgit2.a

# This file is used by Omnibus and CNG to skip the "bundle install"
# step. Both Omnibus and CNG assume it is in the Gitaly root, not in
# _build. Hence the '../' in front.
${SOURCE_DIR}/.ruby-bundle: ${GITALY_RUBY_DIR}/Gemfile.lock ${GITALY_RUBY_DIR}/Gemfile
	${Q}cd ${GITALY_RUBY_DIR} && bundle config # for debugging
	${Q}cd ${GITALY_RUBY_DIR} && bundle install ${BUNDLE_FLAGS}
	${Q}touch $@

${SOURCE_DIR}/NOTICE: ${BUILD_DIR}/NOTICE
	${Q}mv $< $@

${BUILD_DIR}/NOTICE: ${GO_LICENSES} clean-ruby-vendor-go
	${Q}rm -rf ${BUILD_DIR}/licenses
	${Q}GOFLAGS="-tags=${GO_BUILD_TAGS}" ${GO_LICENSES} save ./... --save_path=${BUILD_DIR}/licenses
	# some projects may be copied from the Go module cache
	# (GOPATH/pkg/mod) and retain strict permissions (444). These
	# permissions are not desirable when removing and rebuilding:
	${Q}find ${BUILD_DIR}/licenses -type d -exec chmod 0755 {} \;
	${Q}find ${BUILD_DIR}/licenses -type f -exec chmod 0644 {} \;
	${Q}go run ${SOURCE_DIR}/_support/noticegen/noticegen.go -source ${BUILD_DIR}/licenses -template ${SOURCE_DIR}/_support/noticegen/notice.template > ${BUILD_DIR}/NOTICE

${BUILD_DIR}:
	${Q}mkdir -p ${BUILD_DIR}
${BUILD_DIR}/bin: | ${BUILD_DIR}
	${Q}mkdir -p ${BUILD_DIR}/bin
${BUILD_DIR}/tools: | ${BUILD_DIR}
	${Q}mkdir -p ${BUILD_DIR}/tools
${DEPENDENCY_DIR}: | ${BUILD_DIR}
	${Q}mkdir -p ${DEPENDENCY_DIR}

# This is a build hack to avoid excessive rebuilding of targets. Instead of
# depending on the timestamp of the Makefile, which will change e.g. between
# jobs of a CI pipeline, we start depending on its hash. Like this, we only
# rebuild if the Makefile actually has changed contents.
${BUILD_DIR}/Makefile.sha256: Makefile | ${BUILD_DIR}
	${Q}sha256sum -c $@ >/dev/null 2>&1 || >$@ sha256sum Makefile

# This is in the same spirit as the Makefile.sha256 optimization: we want to
# rebuild only if the dependency's version changes. The dependency on the phony
# target is required to always rebuild these targets.
.PHONY: dependency-version
${DEPENDENCY_DIR}/libgit2.version: dependency-version | ${DEPENDENCY_DIR}
	${Q}[ x"$$(cat "$@" 2>/dev/null)" = x"${LIBGIT2_VERSION}" ] || >$@ echo -n "${LIBGIT2_VERSION}"
${DEPENDENCY_DIR}/git.version: dependency-version | ${DEPENDENCY_DIR}
	${Q}[ x"$$(cat "$@" 2>/dev/null)" = x"${GIT_VERSION}" ] || >$@ echo -n "${GIT_VERSION}"

${LIBGIT2_INSTALL_DIR}/lib/libgit2.a: ${DEPENDENCY_DIR}/libgit2.version
	${Q}if ! [ -d "${LIBGIT2_SOURCE_DIR}" ]; then \
	    ${GIT} clone --depth 1 --branch ${LIBGIT2_VERSION} --quiet ${LIBGIT2_REPO_URL} ${LIBGIT2_SOURCE_DIR}; \
	elif ! git -C "${LIBGIT2_SOURCE_DIR}" rev-parse --quiet --verify ${LIBGIT2_VERSION}^{tree} >/dev/null; then \
	    ${GIT} -C "${LIBGIT2_SOURCE_DIR}" fetch --quiet ${LIBGIT2_REPO_URL} ${LIBGIT2_VERSION}; \
	fi
	${Q}rm -rf ${LIBGIT2_BUILD_DIR}
	${Q}mkdir -p ${LIBGIT2_BUILD_DIR}
	${Q}cd ${LIBGIT2_BUILD_DIR} && cmake ${LIBGIT2_SOURCE_DIR} ${LIBGIT2_BUILD_OPTIONS}
	${Q}CMAKE_BUILD_PARALLEL_LEVEL=$(shell nproc) cmake --build ${LIBGIT2_BUILD_DIR} --target install
	go install -a github.com/libgit2/git2go/${GIT2GO_VERSION}

ifeq (${GIT_USE_PREBUILT_BINARIES},)
${GIT_INSTALL_DIR}/bin/git: ${DEPENDENCY_DIR}/git.version
	${Q}if ! [ -d "${GIT_SOURCE_DIR}" ]; then \
	    ${GIT} clone --depth 1 --branch ${GIT_VERSION} --quiet ${GIT_REPO_URL} ${GIT_SOURCE_DIR}; \
	elif ! git -C "${GIT_SOURCE_DIR}" rev-parse --quiet --verify ${GIT_VERSION}^{tree} >/dev/null; then \
	    ${GIT} -C "${GIT_SOURCE_DIR}" fetch --quiet ${GIT_REPO_URL} ${GIT_VERSION}; \
	fi
	${Q}${GIT} -C "${GIT_SOURCE_DIR}" switch --quiet --detach ${GIT_VERSION}
	${Q}rm -rf ${GIT_INSTALL_DIR}
	${Q}mkdir -p ${GIT_INSTALL_DIR}
	env -u MAKEFLAGS -u GIT_VERSION ${MAKE} -C ${GIT_SOURCE_DIR} -j$(shell nproc) prefix=${GIT_PREFIX} ${GIT_BUILD_OPTIONS} install
else
${DEPENDENCY_DIR}/git_full_bins.tgz: ${DEPENDENCY_DIR}/git.version
	curl -o $@.tmp --silent --show-error -L ${GIT_BINARIES_URL}
	${Q}printf '${GIT_BINARIES_HASH}  $@.tmp' | sha256sum -c -
	${Q}mv $@.tmp $@

${GIT_INSTALL_DIR}/bin/git: ${DEPENDENCY_DIR}/git_full_bins.tgz
	${Q}rm -rf ${GIT_INSTALL_DIR}
	${Q}mkdir -p ${GIT_INSTALL_DIR}
	tar -C ${GIT_INSTALL_DIR} -xvzf ${DEPENDENCY_DIR}/git_full_bins.tgz
endif

${BUILD_DIR}/protoc.zip: ${BUILD_DIR}/Makefile.sha256
	${Q}if [ -z "${PROTOC_URL}" ]; then echo "Cannot generate protos on unsupported platform ${OS}" && exit 1; fi
	curl -o $@.tmp --silent --show-error -L ${PROTOC_URL}
	${Q}printf '${PROTOC_HASH}  $@.tmp' | sha256sum -c -
	${Q}mv $@.tmp $@

${PROTOC}: ${BUILD_DIR}/protoc.zip | ${BUILD_DIR}
	${Q}rm -rf ${BUILD_DIR}/protoc
	${Q}mkdir -p ${BUILD_DIR}/protoc
	cd ${BUILD_DIR}/protoc && unzip ${BUILD_DIR}/protoc.zip

# We're using per-tool go.mod files in order to avoid conflicts in the graph in
# case we used a single go.mod file for all tools.
${BUILD_DIR}/tools/%/go.mod: | ${BUILD_DIR}/tools
	${Q}mkdir -p $(dir $@)
	${Q}cd $(dir $@) && go mod init _build

${BUILD_DIR}/tools/%: GOBIN = ${BUILD_DIR}/tools
${BUILD_DIR}/tools/%: ${BUILD_DIR}/Makefile.sha256 ${BUILD_DIR}/tools/.%/go.mod
	${Q}cd ${BUILD_DIR}/tools/.$(notdir $@) && go get ${TOOL_PACKAGE}

# Tools hosted by Gitaly itself
${GITALYFMT}: | ${BUILD_DIR}/tools
	${Q}go build -o $@ ${SOURCE_DIR}/internal/cmd/gitalyfmt

${PROTOC_GEN_GITALY}: proto | ${BUILD_DIR}/tools
	${Q}go build -o $@ ${SOURCE_DIR}/proto/go/internal/cmd/protoc-gen-gitaly

# External tools
${GOCOVER_COBERTURA}: TOOL_PACKAGE = github.com/t-yuki/gocover-cobertura@${GOCOVER_COBERTURA_VERSION}
${GOIMPORTS}:         TOOL_PACKAGE = golang.org/x/tools/cmd/goimports@2538eef75904eff384a2551359968e40c207d9d2
${GOLANGCI_LINT}:     TOOL_PACKAGE = github.com/golangci/golangci-lint/cmd/golangci-lint@v${GOLANGCI_LINT_VERSION}
${GO_JUNIT_REPORT}:   TOOL_PACKAGE = github.com/jstemmer/go-junit-report@984a47ca6b0a7d704c4b589852051b4d7865aa17
${GO_LICENSES}:       TOOL_PACKAGE = github.com/google/go-licenses@73411c8fa237ccc6a75af79d0a5bc021c9487aad
${PROTOC_GEN_GO}:     TOOL_PACKAGE = github.com/golang/protobuf/protoc-gen-go@v${PROTOC_GEN_GO_VERSION}

${TEST_REPO}:
	${GIT} clone --bare --quiet https://gitlab.com/gitlab-org/gitlab-test.git $@
	# Git notes aren't fetched by default with git clone
	${GIT} -C $@ fetch origin refs/notes/*:refs/notes/*
	rm -rf $@/refs
	mkdir -p $@/refs/heads $@/refs/tags
	cp ${SOURCE_DIR}/_support/gitlab-test.git-packed-refs $@/packed-refs
	${GIT} -C $@ fsck --no-progress

${TEST_REPO_GIT}:
	${GIT} clone --bare --quiet https://gitlab.com/gitlab-org/gitlab-git-test.git $@
	rm -rf $@/refs
	mkdir -p $@/refs/heads $@/refs/tags
	cp ${SOURCE_DIR}/_support/gitlab-git-test.git-packed-refs $@/packed-refs
	${GIT} -C $@ fsck --no-progress
