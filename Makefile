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
export GITALY_TEST_RUBY_DIR := $(BUILD_DIR)/ruby
export BUNDLE_PATH := vendor/bundle

BUILDTIME = $(shell date -u +%Y%m%d.%H%M%S)
VERSION_PREFIXED = $(shell git describe)
VERSION = $(VERSION_PREFIXED:v%=%)
LDFLAGS = -ldflags '-X $(PKG)/internal/version.version=$(VERSION) -X $(PKG)/internal/version.buildtime=$(BUILDTIME)'

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
	go install $(LDFLAGS) $(COMMAND_PACKAGES)
	cp $(foreach cmd,$(COMMANDS),$(BIN_BUILD_DIR)/$(cmd)) $(BUILD_DIR)/

.ruby-bundle:	ruby/Gemfile.lock
	cd ruby && bundle install
	touch $@

.PHONY: install
install: build
	mkdir -p $(INSTALL_DEST_DIR)
	cd $(BIN_BUILD_DIR) && install $(COMMANDS) $(INSTALL_DEST_DIR)

.PHONY: verify
verify: lint check-formatting govendor-status notice-up-to-date

.PHONY: govendor-status
govendor-status: $(TARGET_SETUP) $(GOVENDOR)
	cd $(PKG_BUILD_DIR) && govendor status

$(TEST_REPO):
	git clone --bare https://gitlab.com/gitlab-org/gitlab-test.git $@

.PHONY: test
test: $(TARGET_SETUP) $(TEST_REPO) prepare-tests .ruby-bundle
	@go test $(LOCAL_PACKAGES)

.PHONY: test-changes
test-changes: $(TARGET_SETUP) $(TEST_REPO) prepare-tests .ruby-bundle
	cd $(PKG_BUILD_DIR) && go test $(CHANGED_LOCAL_GO_PACKAGES)

.PHONY: prepare-tests
prepare-tests: $(TARGET_SETUP) $(TEST_REPO)

.PHONY: lint
lint: $(GOLINT)
	go run _support/lint.go

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
cover: $(TARGET_SETUP) $(TEST_REPO) $(GOCOVMERGE)
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
