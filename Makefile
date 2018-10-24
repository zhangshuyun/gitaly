# Top-level Makefile for Gitaly
#
# Responsibilities of this file:
# - create GOPATH in _build with symlink to current dir
# - re-generate _build/Makefile from makegen.go on each run
# - dispatch commands to _build/Makefile
#
# "Magic" should happen in the makegen.go dynamic template. We want
# _build/Makefile to be as static as possible.

TARGET_DIR ?= $(CURDIR)/_build
PKG = gitlab.com/gitlab-org/gitaly
MAKEGEN = $(TARGET_DIR)/makegen
TOP_LEVEL := $(CURDIR)

# These variables are handed down to make in _build
export GOPATH := $(TARGET_DIR)
export PATH := $(PATH):$(GOPATH)/bin
export TEST_REPO_STORAGE_PATH := $(CURDIR)/internal/testhelper/testdata/data

all: build

.PHONY: build
build: prepare-build
	cd $(TARGET_DIR) && make install INSTALL_DEST_DIR=$(CURDIR) 

.PHONY: install
install: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: assemble
assemble: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: binaries
binaries: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: prepare-tests
prepare-tests: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: test
test: prepare-build
	cd $(TARGET_DIR) && make $@
	
.PHONY: rspec
rspec: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: verify
verify: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: format
format: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: cover
cover: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: notice
notice: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: race-go
race-go: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: docker
docker: prepare-build
	cd $(TARGET_DIR) && make $@

.PHONY: prepare-build
prepare-build: $(TARGET_DIR)/.ok update-makefile
$(TARGET_DIR)/.ok:
	mkdir -p $(TARGET_DIR)/src/$(shell dirname $(PKG))
	cd $(TARGET_DIR)/src/$(shell dirname $(PKG)) && rm -f $(shell basename $(PKG)) && \
		ln -sf $(TOP_LEVEL) $(shell basename $(PKG))
	touch $@

.PHONY: update-makefile
update-makefile: $(TARGET_DIR)/makegen $(TARGET_DIR)/.ok
	cd $(TARGET_DIR) && ./makegen > Makefile

$(TARGET_DIR)/makegen: _support/makegen.go $(TARGET_DIR)/.ok
	go build -o $@ _support/makegen.go

clean:
	rm -rf $(TARGET_DIR) .ruby-bundle $(TEST_REPO_STORAGE_PATH)
