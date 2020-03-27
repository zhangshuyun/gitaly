# Top-level Makefile for Gitaly
#
# Responsibilities of this file:
# - create GOPATH in _build with symlink to current dir
# - re-generate _build/Makefile from makegen.go on each run
# - dispatch commands to _build/Makefile
#
# Besides the targets that manage _build and _build/Makefile, all
# targets in this Makefile should look like this:
#
# .PHONY: foobar
# foobar: prepare-build
# 	cd $(BUILD_DIR) && $(MAKE) $@
#
# All other logic should happen in _support/Makefile.template and
# _support/makegen.go.
#

BUILD_DIR = _build
PKG = gitlab.com/gitlab-org/gitaly
MAKEGEN = $(BUILD_DIR)/makegen

# These variables are used by makegen
export SOURCE_DIR := $(CURDIR)

# Used to build _support/makegen.go
export GO111MODULE = on

all: build

.PHONY: build
build: prepare-build
	cd $(BUILD_DIR) && $(MAKE) install INSTALL_DEST_DIR=$(CURDIR)

.PHONY: build-gitaly-remote
build-gitaly-remote: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: test-gitaly-remote
test-gitaly-remote: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: install
install: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: assemble
assemble: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: binaries
binaries: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: prepare-tests
prepare-tests: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: test
test: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: test-go-flaky
test-go-flaky: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: test-with-praefect
test-with-praefect: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: test-with-proxies
test-with-proxies: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: rspec
rspec: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: rspec-gitlab-shell
rspec-gitlab-shell: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: test-postgres
test-postgres: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: verify
verify: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: lint
lint: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: lint-warnings
lint-warnings: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: format
format: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: cover
cover: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: notice
notice: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: race-go
race-go: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: docker
docker: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: proto
proto: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: no-changes
no-changes: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: smoke-test
smoke-test: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: prepare-build
prepare-build: $(BUILD_DIR)/.ok update-makefile
$(BUILD_DIR)/.ok:
	mkdir -p $(BUILD_DIR)
	touch $@

.PHONY: update-makefile
update-makefile: _build/makegen $(BUILD_DIR)/.ok
	cd $(BUILD_DIR) && ./makegen > Makefile

# This go.mod file soaks up go.mod/go.sum changes that we don't want in the top-level go.mod.
$(BUILD_DIR)/go.mod: $(BUILD_DIR)/.ok
	(cd $(BUILD_DIR) && go mod init _build)

_build/makegen: _support/makegen.go $(BUILD_DIR)/go.mod
	cd $(BUILD_DIR) && go build -o $(CURDIR)/$@ $(SOURCE_DIR)/_support/makegen.go

clean:
	git clean -fdX
