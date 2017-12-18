PREFIX := /usr/local
PKG := gitlab.com/gitlab-org/gitaly
BUILD_DIR := $(CURDIR)
TARGET_DIR := $(BUILD_DIR)/_build
TARGET_SETUP := $(TARGET_DIR)/.ok
BIN_BUILD_DIR := $(TARGET_DIR)/bin
PKG_BUILD_DIR := $(TARGET_DIR)/src/$(PKG)
export TEST_REPO_STORAGE_PATH := $(BUILD_DIR)/internal/testhelper/testdata/data
TEST_REPO := $(TEST_REPO_STORAGE_PATH)/gitlab-test.git
INSTALL_DEST_DIR := $(DESTDIR)$(PREFIX)/bin/
COVERAGE_DIR := $(TARGET_DIR)/cover
ASSEMBLY_ROOT := $(TARGET_DIR)/assembly
export GITALY_TEST_RUBY_DIR := $(BUILD_DIR)/ruby
BUNDLE_FLAGS ?= --deployment

BUILDTIME = $(shell date -u +%Y%m%d.%H%M%S)
VERSION_PREFIXED = $(shell git describe)
VERSION = $(VERSION_PREFIXED:v%=%)
GO_LDFLAGS = -ldflags '-X $(PKG)/internal/version.version=$(VERSION) -X $(PKG)/internal/version.buildtime=$(BUILDTIME)'

unexport GOROOT
unexport GOBIN

export GOPATH := $(TARGET_DIR)
export PATH := $(GOPATH)/bin:$(PATH)

# Returns a list of all non-vendored (local packages)
LOCAL_PACKAGES = $(shell cd "$(PKG_BUILD_DIR)" && GOPATH=$(GOPATH) go list ./... | grep -v '^$(PKG)/vendor/')
LOCAL_GO_FILES = $(shell find -L $(PKG_BUILD_DIR)  -name "*.go" -not -path "$(PKG_BUILD_DIR)/vendor/*" -not -path "$(PKG_BUILD_DIR)/_build/*")
CHANGED_LOCAL_GO_FILES = $(shell git status  --porcelain --short | awk '{ print $$2 }' | grep -v '^$(PKG)/vendor/' | grep .go$)
CHANGED_LOCAL_GO_PACKAGES = $(foreach file,$(CHANGED_LOCAL_GO_FILES),./$(dir $(file))/...)
COMMAND_PACKAGES = $(shell cd "$(PKG_BUILD_DIR)" && GOPATH=$(GOPATH) go list ./cmd/...)
COMMANDS = $(subst $(PKG)/cmd/,,$(COMMAND_PACKAGES))

# Developer Tools
GOVENDOR = $(BIN_BUILD_DIR)/govendor
GOLINT = $(BIN_BUILD_DIR)/golint
GOCOVMERGE = $(BIN_BUILD_DIR)/gocovmerge
GOIMPORTS = $(BIN_BUILD_DIR)/goimports
MEGACHECK = $(BIN_BUILD_DIR)/megacheck

.NOTPARALLEL:

.PHONY: all
all: build

$(TARGET_SETUP):
	rm -rf $(TARGET_DIR)
	mkdir -p "$(dir $(PKG_BUILD_DIR))"
	ln -sf ../../../.. "$(PKG_BUILD_DIR)"
	mkdir -p "$(BIN_BUILD_DIR)"
	touch "$(TARGET_SETUP)"

build:	.ruby-bundle $(TARGET_SETUP)
	go install $(GO_LDFLAGS) $(COMMAND_PACKAGES)
	cp $(foreach cmd,$(COMMANDS),$(BIN_BUILD_DIR)/$(cmd)) $(BUILD_DIR)/

.ruby-bundle:	ruby/Gemfile.lock ruby/Gemfile
	cd ruby && bundle install $(BUNDLE_FLAGS)
	touch $@

# TODO: confirm what references this target? Omnibus? Source installs?
.PHONY: install
install: build
	mkdir -p $(INSTALL_DEST_DIR)
	cd $(BIN_BUILD_DIR) && install $(COMMANDS) $(INSTALL_DEST_DIR)

.PHONY: force-ruby-bundle
force-ruby-bundle:
	rm -f .ruby-bundle

# Assembles all runtime components into a directory
# Used by the GDK: run `make assemble ASSEMBLY_ROOT=.../gitaly`
.PHONY: assemble
assemble: force-ruby-bundle build
	rm -rf $(ASSEMBLY_ROOT)/bin $(ASSEMBLY_ROOT)/ruby
	mkdir -p $(ASSEMBLY_ROOT)/bin
	cp -r ruby $(ASSEMBLY_ROOT)/ruby
	install $(foreach cmd,$(COMMANDS),$(BIN_BUILD_DIR)/$(cmd)) $(ASSEMBLY_ROOT)/bin

binaries: assemble
	@if [ $$(uname -m) != 'x86_64' ]; then echo Incorrect architecture for build: $(uname -m); exit 1; fi
	@cd $(ASSEMBLY_ROOT) && shasum -a 256 bin/* | tee checksums.sha256.txt

docker: $(TARGET_SETUP)
	rm -rf $(TARGET_DIR)/docker/
	mkdir -p $(TARGET_DIR)/docker/bin/
	cp -r ruby $(TARGET_DIR)/docker/ruby/
	rm -rf $(TARGET_DIR)/docker/ruby/vendor/bundle

	for cmd in $(COMMAND_PACKAGES); do \
		GOOS=linux GOARCH=amd64 go build $(GO_LDFLAGS) -o "$(TARGET_DIR)/docker/bin/$$(basename $$cmd)" $$cmd; \
	done

	cp Dockerfile $(TARGET_DIR)/docker/
	docker build -t gitlab/gitaly:$(VERSION_PREFIXED) -t gitlab/gitaly:latest $(TARGET_DIR)/docker/

.PHONY: verify
verify: lint check-formatting megacheck govendor-status notice-up-to-date

.PHONY: govendor-status
govendor-status: $(TARGET_SETUP) $(GOVENDOR)
	cd $(PKG_BUILD_DIR) && govendor status

$(TEST_REPO):
	git clone --bare https://gitlab.com/gitlab-org/gitlab-test.git $@

.PHONY: prepare-tests
prepare-tests: $(TARGET_SETUP) $(TEST_REPO) .ruby-bundle

.PHONY: test
test: prepare-tests
	@go test $(LOCAL_PACKAGES)

.PHONY: test-changes
test-changes: prepare-tests
	cd $(PKG_BUILD_DIR) && go test $(CHANGED_LOCAL_GO_PACKAGES)

.PHONY: lint
lint: $(GOLINT)
	go run _support/lint.go

.PHONY: megacheck
megacheck: $(MEGACHECK)
	@$(MEGACHECK) $(LOCAL_PACKAGES)

.PHONY: check-formatting
check-formatting: $(TARGET_SETUP) $(GOIMPORTS)
	@test -z "$$($(GOIMPORTS) -e -l $(LOCAL_GO_FILES))" || (echo >&2 "Formatting or imports need fixing: 'make format'" && $(GOIMPORTS) -e -l $(LOCAL_GO_FILES) && false)

.PHONY: format
format: $(TARGET_SETUP) $(GOIMPORTS)
    # In addition to fixing imports, goimports also formats your code in the same style as gofmt
	# so it can be used as a replacement.
	@$(GOIMPORTS) -w -l $(LOCAL_GO_FILES)

.PHONY: package
package: build
	./_support/package/package $(COMMANDS)

.PHONY: notice
notice: $(TARGET_SETUP) $(GOVENDOR)
	cd $(PKG_BUILD_DIR) && govendor license -template _support/notice.template -o $(BUILD_DIR)/NOTICE

.PHONY: notice-up-to-date
notice-up-to-date: $(TARGET_SETUP) $(GOVENDOR)
	@(cd $(PKG_BUILD_DIR) && govendor license -template _support/notice.template | cmp - NOTICE) || (echo >&2 "NOTICE requires update: 'make notice'" && false)

.PHONY: codeclimate-report
codeclimate-report:
	docker run --env CODECLIMATE_CODE="$(BUILD_DIR)" --volume "$(BUILD_DIR)":/code --volume /var/run/docker.sock:/var/run/docker.sock --volume /tmp/cc:/tmp/cc codeclimate/codeclimate analyze -f text

.PHONY: clean
clean:
	rm -rf $(TARGET_DIR) $(TEST_REPO) $(TEST_REPO_STORAGE_PATH) ./internal/service/ssh/gitaly-*-pack .ruby-bundle

.PHONY: cover
cover: prepare-tests $(GOCOVMERGE)
	@echo "NOTE: make cover does not exit 1 on failure, don't use it to check for tests success!"
	mkdir -p "$(COVERAGE_DIR)"
	rm -f $(COVERAGE_DIR)/*.out "$(COVERAGE_DIR)/all.merged" "$(COVERAGE_DIR)/all.html"
	echo $(LOCAL_PACKAGES) > $(TARGET_DIR)/local_packages
	for MOD in `cat $(TARGET_DIR)/local_packages`; do \
		go test -coverpkg=`cat $(TARGET_DIR)/local_packages |tr " " "," ` \
			-coverprofile=$(COVERAGE_DIR)/unit-`echo $$MOD|tr "/" "_"`.out \
			$$MOD 2>&1 | grep -v "no packages being tested depend on"; \
	done
	$(GOCOVMERGE) $(COVERAGE_DIR)/*.out > "$(COVERAGE_DIR)/all.merged"
	go tool cover -html  "$(COVERAGE_DIR)/all.merged" -o "$(COVERAGE_DIR)/all.html"
	@echo ""
	@echo "=====> Total test coverage: <====="
	@echo ""
	@go tool cover -func "$(COVERAGE_DIR)/all.merged"

# Install govendor
$(GOVENDOR): $(TARGET_SETUP)
	go get -v github.com/kardianos/govendor

# Install golint
$(GOLINT): $(TARGET_SETUP)
	go get -v github.com/golang/lint/golint

# Install gocovmerge
$(GOCOVMERGE): $(TARGET_SETUP)
	go get -v github.com/wadey/gocovmerge

# Install goimports
$(GOIMPORTS): $(TARGET_SETUP)
	go get -v golang.org/x/tools/cmd/goimports

# Install megacheck
$(MEGACHECK): $(TARGET_SETUP)
	go get -v honnef.co/go/tools/cmd/megacheck
