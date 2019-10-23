# Top-level Makefile for Gitaly
#
# Responsibilities of this file:
# - create GOPATH in _build with symlink to current dir
# - re-generate _build/Makefile from makegen.go on each run
# - dispatch commands to _build/Makefile
#
# "Magic" should happen in the makegen.go dynamic template. We want
# _build/Makefile to be as static as possible.

BUILD_DIR = _build
PKG = gitlab.com/gitlab-org/gitaly
MAKEGEN = $(BUILD_DIR)/makegen

# These variables are handed down to make in _build
export PATH := $(BUILD_DIR)/bin:$(PATH)
export TEST_REPO_STORAGE_PATH := $(CURDIR)/internal/testhelper/testdata/data
export SOURCE_DIR := $(CURDIR)
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
	
.PHONY: test-with-proxies
test-with-proxies: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: rspec
rspec: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: rspec-gitlab-shell
rspec-gitlab-shell: prepare-build
	cd $(BUILD_DIR) && $(MAKE) $@

.PHONY: verify
verify: prepare-build
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
	mkdir -p $(BUILD_DIR)/src/$(shell dirname $(PKG))
	cd $(BUILD_DIR)/src/$(shell dirname $(PKG)) && rm -f $(shell basename $(PKG)) && \
		ln -sf ../../../.. $(shell basename $(PKG))
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
	rm -rf $(BUILD_DIR)
	git clean -fdX
