# Makefile for Gitaly

# You can override options by creating a "config.mak" file in Gitaly's root
# directory.
-include config.mak

# Unexport environment variables which have an effect on Git itself.
# We need to keep GIT_PREFIX because it's used to determine where our
# self-built Git should be installed into. It's probably not going to
# matter much though.
unexport $(filter-out GIT_PREFIX,$(shell git rev-parse --local-env-vars))

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
TOOLS_DIR        := ${BUILD_DIR}/tools
GITALY_RUBY_DIR  := ${SOURCE_DIR}/ruby
MODULE_VERSION   := $(notdir $(shell go list -m))

# These variables may be overridden at runtime by top-level make
PREFIX           ?= /usr/local
prefix           ?= ${PREFIX}
exec_prefix      ?= ${prefix}
bindir           ?= ${exec_prefix}/bin
INSTALL_DEST_DIR := ${DESTDIR}${bindir}
GIT_PREFIX       ?= ${GIT_INSTALL_DIR}

# Tools
GIT               := $(shell which git)
GOIMPORTS         := ${TOOLS_DIR}/goimports
GITALYFMT         := ${TOOLS_DIR}/gitalyfmt
GOLANGCI_LINT     := ${TOOLS_DIR}/golangci-lint
GO_LICENSES       := ${TOOLS_DIR}/go-licenses
PROTOC            := ${TOOLS_DIR}/protoc/bin/protoc
PROTOC_GEN_GO     := ${TOOLS_DIR}/protoc-gen-go
PROTOC_GEN_GO_GRPC:= ${TOOLS_DIR}/protoc-gen-go-grpc
PROTOC_GEN_GITALY := ${TOOLS_DIR}/protoc-gen-gitaly
GO_JUNIT_REPORT   := ${TOOLS_DIR}/go-junit-report
GOCOVER_COBERTURA := ${TOOLS_DIR}/gocover-cobertura

# Tool options
GOLANGCI_LINT_OPTIONS ?=
GOLANGCI_LINT_CONFIG  ?= ${SOURCE_DIR}/.golangci.yml

# Build information
BUNDLE_DEPLOYMENT ?= $(shell test -f ${SOURCE_DIR}/../.gdk-install-root && echo false || echo true)
GITALY_PACKAGE    := gitlab.com/gitlab-org/gitaly/v14
BUILD_TIME        := $(shell date +"%Y%m%d.%H%M%S")
GITALY_VERSION    := $(shell ${GIT} describe --match v* 2>/dev/null | sed 's/^v//' || cat ${SOURCE_DIR}/VERSION 2>/dev/null || echo unknown)
GO_LDFLAGS        := -ldflags '-X ${GITALY_PACKAGE}/internal/version.version=${GITALY_VERSION} -X ${GITALY_PACKAGE}/internal/version.buildtime=${BUILD_TIME} -X ${GITALY_PACKAGE}/internal/version.moduleVersion=${MODULE_VERSION}'
GO_BUILD_TAGS     := tracer_static,tracer_static_jaeger,continuous_profiler_stackdriver,static,system_libgit2

# Dependency versions
GOLANGCI_LINT_VERSION     ?= 1.39.0
GOCOVER_COBERTURA_VERSION ?= aaee18c8195c3f2d90e5ef80ca918d265463842a
GOIMPORTS_VERSION         ?= 2538eef75904eff384a2551359968e40c207d9d2
GO_JUNIT_REPORT_VERSION   ?= 984a47ca6b0a7d704c4b589852051b4d7865aa17
GO_LICENSES_VERSION       ?= 73411c8fa237ccc6a75af79d0a5bc021c9487aad
# https://pkg.go.dev/github.com/protocolbuffers/protobuf
PROTOC_VERSION            ?= 3.17.3
# https://pkg.go.dev/google.golang.org/protobuf
PROTOC_GEN_GO_VERSION     ?= 1.26.0
# https://pkg.go.dev/google.golang.org/grpc/cmd/protoc-gen-go-grpc
PROTOC_GEN_GO_GRPC_VERSION?= 1.1.0
GIT_VERSION               ?= v2.32.0
GIT2GO_VERSION            ?= v31
LIBGIT2_VERSION           ?= v1.1.0

# Dependency downloads
ifeq (${OS},Darwin)
    PROTOC_URL            ?= https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-osx-x86_64.zip
    PROTOC_HASH           ?= 68901eb7ef5b55d7f2df3241ab0b8d97ee5192d3902c59e7adf461adc058e9f1
else ifeq (${OS},Linux)
    PROTOC_URL            ?= https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip
    PROTOC_HASH           ?= d4246a5136cf9cd1abc851c521a1ad6b8884df4feded8b9cbd5e2a2226d4b357
endif

# Git target
GIT_REPO_URL      ?= https://gitlab.com/gitlab-org/gitlab-git.git
GIT_INSTALL_DIR   := ${DEPENDENCY_DIR}/git/install
GIT_SOURCE_DIR    := ${DEPENDENCY_DIR}/git/source
GIT_QUIET         :=
ifeq (${Q},@)
	GIT_QUIET = --quiet
endif

ifndef GIT_PATCHES
    # Before adding custom patches, please read doc/PROCESS.md#Patching-git
    # first to make sure your patches meet our acceptance criteria. Patches
    # must be put into `_support/git-patches`.
    #GIT_PATCHES +=
endif

ifndef GIT_BUILD_OPTIONS
    # activate developer checks
    GIT_BUILD_OPTIONS += DEVELOPER=1
    # but don't cause warnings to fail the build
    GIT_BUILD_OPTIONS += DEVOPTS=no-error
    GIT_BUILD_OPTIONS += USE_LIBPCRE=YesPlease
    GIT_BUILD_OPTIONS += NO_PERL=YesPlease
    GIT_BUILD_OPTIONS += NO_EXPAT=YesPlease
    GIT_BUILD_OPTIONS += NO_TCLTK=YesPlease
    GIT_BUILD_OPTIONS += NO_GETTEXT=YesPlease
    GIT_BUILD_OPTIONS += NO_PYTHON=YesPlease
    GIT_BUILD_OPTIONS += NO_INSTALL_HARDLINKS=YesPlease
endif

# libgit2 target
LIBGIT2_REPO_URL    ?= https://gitlab.com/libgit2/libgit2
LIBGIT2_SOURCE_DIR  ?= ${DEPENDENCY_DIR}/libgit2/source
LIBGIT2_BUILD_DIR   ?= ${DEPENDENCY_DIR}/libgit2/build
LIBGIT2_INSTALL_DIR ?= ${DEPENDENCY_DIR}/libgit2/install

ifndef LIBGIT2_BUILD_OPTIONS
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
TEST_PACKAGES    ?= ${SOURCE_DIR}/...
TEST_OPTIONS     ?= -v -count=1
TEST_REPORT_DIR  ?= ${BUILD_DIR}/reports
TEST_OUTPUT_NAME ?= go-${GO_VERSION}-git-${GIT_VERSION}
TEST_OUTPUT      ?= ${TEST_REPORT_DIR}/go-tests-output-${TEST_OUTPUT_NAME}.txt
TEST_REPORT      ?= ${TEST_REPORT_DIR}/go-tests-report-${TEST_OUTPUT_NAME}.xml
TEST_EXIT        ?= ${TEST_REPORT_DIR}/go-tests-exit-${TEST_OUTPUT_NAME}.txt
TEST_REPO_DIR    := ${BUILD_DIR}/testrepos
TEST_REPO        := ${TEST_REPO_DIR}/gitlab-test.git
TEST_REPO_GIT    := ${TEST_REPO_DIR}/gitlab-git-test.git
BENCHMARK_REPO   := ${TEST_REPO_DIR}/benchmark.git

# Find all commands.
find_commands         = $(notdir $(shell find ${SOURCE_DIR}/cmd -mindepth 1 -maxdepth 1 -type d -print))
# Find all command binaries.
find_command_binaries = $(addprefix ${BUILD_DIR}/bin/, $(shell ls ${BUILD_DIR}/bin))
# Find all Go source files.
find_go_sources       = $(shell find ${SOURCE_DIR} -type d \( -name ruby -o -name vendor -o -name testdata -o -name '_*' -o -path '*/proto/go' \) -prune -o -type f -name '*.go' -not -name '*.pb.go' -print | sort -u)

# run_go_tests will execute Go tests with all required parameters. Its
# behaviour can be modified via the following variables:
#
# GO_BUILD_TAGS: tags used to build the executables
# TEST_OPTIONS: any additional options
# TEST_PACKAGES: packages which shall be tested
run_go_tests = PATH='${SOURCE_DIR}/internal/testhelper/testdata/home/bin:${PATH}' \
    GIT_DIR=/dev/null \
    go test ${GO_LDFLAGS} -tags '${GO_BUILD_TAGS}' ${TEST_OPTIONS} ${TEST_PACKAGES}

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

# By default, intermediate targets get deleted automatically after a successful
# build. We do not want that though: there's some precious intermediate targets
# like our `*.version` targets which are required in order to determine whether
# a dependency needs to be rebuilt. By specifying `.SECONDARY`, intermediate
# targets will never get deleted automatically.
.SECONDARY:

.PHONY: all
all: INSTALL_DEST_DIR = ${SOURCE_DIR}
all: install

.PHONY: build
build: ${SOURCE_DIR}/.ruby-bundle libgit2
	go install ${GO_LDFLAGS} -tags "${GO_BUILD_TAGS}" $(addprefix ${GITALY_PACKAGE}/cmd/, $(call find_commands))
	# We use version suffix for the gitaly-git2go binary to support compatibility contract between
	# gitaly and gitaly-git2go during upgrade deployment.
	# For more information refer to https://gitlab.com/gitlab-org/gitaly/-/issues/3647#note_599082033
	cp ${BUILD_DIR}/bin/gitaly-git2go "${BUILD_DIR}/bin/gitaly-git2go-${MODULE_VERSION}"

.PHONY: install
install: build
	${Q}mkdir -p ${INSTALL_DEST_DIR}
	install $(call find_command_binaries) ${INSTALL_DEST_DIR}

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

.PHONY: test
bench: TEST_OPTIONS := ${TEST_OPTIONS} -bench=. -run=^$
bench: ${BENCHMARK_REPO} test-go

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
test-postgres: TEST_PACKAGES := gitlab.com/gitlab-org/gitaly/v14/internal/praefect/...
test-postgres: prepare-tests
	${Q}$(call run_go_tests)

.PHONY: race-go
race-go: TEST_OPTIONS := ${TEST_OPTIONS} -race
race-go: test-go

.PHONY: rspec
rspec: build prepare-tests
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
proto: SHARED_PROTOC_OPTS = --plugin=${PROTOC_GEN_GO} --plugin=${PROTOC_GEN_GO_GRPC} --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
proto: ${PROTOC} ${PROTOC_GEN_GO} ${PROTOC_GEN_GO_GRPC} ${SOURCE_DIR}/.ruby-bundle
	${Q}mkdir -p ${SOURCE_DIR}/proto/go/gitalypb
	${Q}rm -f ${SOURCE_DIR}/proto/go/gitalypb/*.pb.go
	${PROTOC} ${SHARED_PROTOC_OPTS} -I ${SOURCE_DIR}/proto --go_out=${SOURCE_DIR}/proto/go/gitalypb --go-grpc_out=${SOURCE_DIR}/proto/go/gitalypb ${SOURCE_DIR}/proto/*.proto
	${SOURCE_DIR}/_support/generate-proto-ruby
	${Q}# this part is related to the generation of sources from testing proto files
	${PROTOC} ${SHARED_PROTOC_OPTS} -I ${SOURCE_DIR}/internal --go_out=${SOURCE_DIR}/internal --go-grpc_out=${SOURCE_DIR}/internal ${SOURCE_DIR}/internal/praefect/grpc-proxy/testdata/test.proto
	${PROTOC} ${SHARED_PROTOC_OPTS} -I ${SOURCE_DIR}/proto -I ${SOURCE_DIR}/internal --go_out=${SOURCE_DIR}/internal --go-grpc_out=${SOURCE_DIR}/internal \
		${SOURCE_DIR}/internal/praefect/mock/mock.proto \
		${SOURCE_DIR}/internal/middleware/cache/testdata/stream.proto \
		${SOURCE_DIR}/internal/helper/chunk/testdata/test.proto \
		${SOURCE_DIR}/internal/middleware/limithandler/testdata/test.proto
	${PROTOC} ${SHARED_PROTOC_OPTS} -I ${SOURCE_DIR}/proto --go_out=${SOURCE_DIR}/proto --go-grpc_out=${SOURCE_DIR}/proto ${SOURCE_DIR}/proto/go/internal/linter/testdata/*.proto

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

.PHONY: upgrade-module
upgrade-module:
	${Q}go run ${SOURCE_DIR}/_support/module-updater/main.go -dir . -from=${FROM_MODULE} -to=${TO_MODULE}
	${Q}${MAKE} proto

.PHONY: git
git: ${GIT_INSTALL_DIR}/bin/git

.PHONY: libgit2
libgit2: ${LIBGIT2_INSTALL_DIR}/lib/libgit2.a

# This file is used by Omnibus and CNG to skip the "bundle install"
# step. Both Omnibus and CNG assume it is in the Gitaly root, not in
# _build. Hence the '../' in front.
${SOURCE_DIR}/.ruby-bundle: ${GITALY_RUBY_DIR}/Gemfile.lock ${GITALY_RUBY_DIR}/Gemfile
	${Q}cd ${GITALY_RUBY_DIR} && bundle config set --local deployment "${BUNDLE_DEPLOYMENT}"
	${Q}cd ${GITALY_RUBY_DIR} && bundle config # for debugging
	${Q}cd ${GITALY_RUBY_DIR} && bundle install
	${Q}touch $@

${SOURCE_DIR}/NOTICE: ${BUILD_DIR}/NOTICE
	${Q}mv $< $@

${BUILD_DIR}/NOTICE: ${GO_LICENSES} clean-ruby-vendor-go
	${Q}rm -rf ${BUILD_DIR}/licenses
	${Q}GOOS=linux GOFLAGS="-tags=${GO_BUILD_TAGS}" ${GO_LICENSES} save ./... --save_path=${BUILD_DIR}/licenses
	${Q}# some projects may be copied from the Go module cache
	${Q}# (GOPATH/pkg/mod) and retain strict permissions (444). These
	${Q}# permissions are not desirable when removing and rebuilding:
	${Q}find ${BUILD_DIR}/licenses -type d -exec chmod 0755 {} \;
	${Q}find ${BUILD_DIR}/licenses -type f -exec chmod 0644 {} \;
	${Q}go run ${SOURCE_DIR}/_support/noticegen/noticegen.go -source ${BUILD_DIR}/licenses -template ${SOURCE_DIR}/_support/noticegen/notice.template > ${BUILD_DIR}/NOTICE

${BUILD_DIR}:
	${Q}mkdir -p ${BUILD_DIR}
${BUILD_DIR}/bin: | ${BUILD_DIR}
	${Q}mkdir -p ${BUILD_DIR}/bin
${TOOLS_DIR}: | ${BUILD_DIR}
	${Q}mkdir -p ${TOOLS_DIR}
${DEPENDENCY_DIR}: | ${BUILD_DIR}
	${Q}mkdir -p ${DEPENDENCY_DIR}

# This is a build hack to avoid excessive rebuilding of targets. Instead of
# depending on the Makefile, we start to depend on tool versions as defined in
# the Makefile. Like this, we only rebuild if the tool versions actually
# change. The dependency on the phony target is required to always rebuild
# these targets.
.PHONY: dependency-version
${DEPENDENCY_DIR}/libgit2.version: dependency-version | ${DEPENDENCY_DIR}
	${Q}[ x"$$(cat "$@" 2>/dev/null)" = x"${LIBGIT2_VERSION} ${LIBGIT2_BUILD_OPTIONS}" ] || >$@ echo -n "${LIBGIT2_VERSION} ${LIBGIT2_BUILD_OPTIONS}"
${DEPENDENCY_DIR}/git.version: dependency-version | ${DEPENDENCY_DIR}
	${Q}[ x"$$(cat "$@" 2>/dev/null)" = x"${GIT_VERSION} ${GIT_BUILD_OPTIONS} ${GIT_PATCHES}" ] || >$@ echo -n "${GIT_VERSION} ${GIT_BUILD_OPTIONS} ${GIT_PATCHES}"
${TOOLS_DIR}/%.version: dependency-version | ${TOOLS_DIR}
	${Q}[ x"$$(cat "$@" 2>/dev/null)" = x"${TOOL_VERSION}" ] || >$@ echo -n "${TOOL_VERSION}"

${LIBGIT2_INSTALL_DIR}/lib/libgit2.a: ${DEPENDENCY_DIR}/libgit2.version
	${Q}${GIT} -c init.defaultBranch=master init ${GIT_QUIET} ${LIBGIT2_SOURCE_DIR}
	${Q}${GIT} -C "${LIBGIT2_SOURCE_DIR}" config remote.origin.url ${LIBGIT2_REPO_URL}
	${Q}${GIT} -C "${LIBGIT2_SOURCE_DIR}" config remote.origin.tagOpt --no-tags
	${Q}${GIT} -C "${LIBGIT2_SOURCE_DIR}" fetch --depth 1 ${GIT_QUIET} origin ${LIBGIT2_VERSION}
	${Q}${GIT} -C "${LIBGIT2_SOURCE_DIR}" checkout ${GIT_QUIET} --detach FETCH_HEAD
	${Q}rm -rf ${LIBGIT2_BUILD_DIR}
	${Q}mkdir -p ${LIBGIT2_BUILD_DIR}
	${Q}cd ${LIBGIT2_BUILD_DIR} && cmake ${LIBGIT2_SOURCE_DIR} ${LIBGIT2_BUILD_OPTIONS}
	${Q}CMAKE_BUILD_PARALLEL_LEVEL=$(shell nproc) cmake --build ${LIBGIT2_BUILD_DIR} --target install
	go install -a github.com/libgit2/git2go/${GIT2GO_VERSION}

${GIT_INSTALL_DIR}/bin/git: ${DEPENDENCY_DIR}/git.version
	${Q}${GIT} -c init.defaultBranch=master init ${GIT_QUIET} ${GIT_SOURCE_DIR}
	${Q}${GIT} -C "${GIT_SOURCE_DIR}" config remote.origin.url ${GIT_REPO_URL}
	${Q}${GIT} -C "${GIT_SOURCE_DIR}" config remote.origin.tagOpt --no-tags
	${Q}${GIT} -C "${GIT_SOURCE_DIR}" fetch --depth 1 ${GIT_QUIET} origin ${GIT_VERSION}
	${Q}${GIT} -C "${GIT_SOURCE_DIR}" reset --hard
	${Q}${GIT} -C "${GIT_SOURCE_DIR}" checkout ${GIT_QUIET} --detach FETCH_HEAD
ifneq (${GIT_PATCHES},)
	${Q}${GIT} -C "${GIT_SOURCE_DIR}" apply $(addprefix "${SOURCE_DIR}"/_support/git-patches/,${GIT_PATCHES})
endif
	${Q}rm -rf ${GIT_INSTALL_DIR}
	${Q}mkdir -p ${GIT_INSTALL_DIR}
	env -u PROFILE -u MAKEFLAGS -u GIT_VERSION ${MAKE} -C ${GIT_SOURCE_DIR} -j$(shell nproc) prefix=${GIT_PREFIX} ${GIT_BUILD_OPTIONS} install

${TOOLS_DIR}/protoc.zip: TOOL_VERSION = ${PROTOC_VERSION}
${TOOLS_DIR}/protoc.zip: ${TOOLS_DIR}/protoc.version
	${Q}if [ -z "${PROTOC_URL}" ]; then echo "Cannot generate protos on unsupported platform ${OS}" && exit 1; fi
	curl -o $@.tmp --silent --show-error -L ${PROTOC_URL}
	${Q}printf '${PROTOC_HASH}  $@.tmp' | sha256sum -c -
	${Q}mv $@.tmp $@

${PROTOC}: ${TOOLS_DIR}/protoc.zip
	${Q}rm -rf ${TOOLS_DIR}/protoc
	${Q}unzip -DD -q -d ${TOOLS_DIR}/protoc ${TOOLS_DIR}/protoc.zip

# We're using per-tool go.mod files in order to avoid conflicts in the graph in
# case we used a single go.mod file for all tools.
${TOOLS_DIR}/%/go.mod: | ${TOOLS_DIR}
	${Q}mkdir -p $(dir $@)
	${Q}cd $(dir $@) && go mod init _build

${TOOLS_DIR}/%: GOBIN = ${TOOLS_DIR}
${TOOLS_DIR}/%: ${TOOLS_DIR}/%.version ${TOOLS_DIR}/.%/go.mod
	${Q}cd ${TOOLS_DIR}/.$* && go get ${TOOL_PACKAGE}@${TOOL_VERSION}

# Tools hosted by Gitaly itself
${GITALYFMT}: | ${TOOLS_DIR}
	${Q}go build -o $@ ${SOURCE_DIR}/internal/cmd/gitalyfmt

${PROTOC_GEN_GITALY}: proto | ${TOOLS_DIR}
	${Q}go build -o $@ ${SOURCE_DIR}/proto/go/internal/cmd/protoc-gen-gitaly

# External tools
${GOCOVER_COBERTURA}: TOOL_PACKAGE = github.com/t-yuki/gocover-cobertura
${GOCOVER_COBERTURA}: TOOL_VERSION = ${GOCOVER_COBERTURA_VERSION}
${GOIMPORTS}:         TOOL_PACKAGE = golang.org/x/tools/cmd/goimports
${GOIMPORTS}:         TOOL_VERSION = ${GOIMPORTS_VERSION}
${GOLANGCI_LINT}:     TOOL_PACKAGE = github.com/golangci/golangci-lint/cmd/golangci-lint
${GOLANGCI_LINT}:     TOOL_VERSION = v${GOLANGCI_LINT_VERSION}
${GO_JUNIT_REPORT}:   TOOL_PACKAGE = github.com/jstemmer/go-junit-report
${GO_JUNIT_REPORT}:   TOOL_VERSION = ${GO_JUNIT_REPORT_VERSION}
${GO_LICENSES}:       TOOL_PACKAGE = github.com/google/go-licenses
${GO_LICENSES}:       TOOL_VERSION = ${GO_LICENSES_VERSION}
${PROTOC_GEN_GO}:     TOOL_PACKAGE = google.golang.org/protobuf/cmd/protoc-gen-go
${PROTOC_GEN_GO}:     TOOL_VERSION = v${PROTOC_GEN_GO_VERSION}
${PROTOC_GEN_GO_GRPC}:TOOL_PACKAGE = google.golang.org/grpc/cmd/protoc-gen-go-grpc
${PROTOC_GEN_GO_GRPC}:TOOL_VERSION = v${PROTOC_GEN_GO_GRPC_VERSION}

${TEST_REPO}:
	${GIT} clone --bare ${GIT_QUIET} https://gitlab.com/gitlab-org/gitlab-test.git $@
	# Git notes aren't fetched by default with git clone
	${GIT} -C $@ fetch origin refs/notes/*:refs/notes/*
	rm -rf $@/refs
	mkdir -p $@/refs/heads $@/refs/tags
	cp ${SOURCE_DIR}/_support/gitlab-test.git-packed-refs $@/packed-refs
	${GIT} -C $@ fsck --no-progress

${TEST_REPO_GIT}:
	${GIT} clone --bare ${GIT_QUIET} https://gitlab.com/gitlab-org/gitlab-git-test.git $@
	rm -rf $@/refs
	mkdir -p $@/refs/heads $@/refs/tags
	cp ${SOURCE_DIR}/_support/gitlab-git-test.git-packed-refs $@/packed-refs
	${GIT} -C $@ fsck --no-progress

${BENCHMARK_REPO}:
	${GIT} clone --bare ${GIT_QUIET} https://gitlab.com/gitlab-org/gitlab.git $@
