PREFIX=/usr/local
PKG=gitlab.com/gitlab-org/gitaly
BUILD_DIR=$(shell pwd)
BIN_BUILD_DIR=${BUILD_DIR}/_build/bin
PKG_BUILD_DIR:=${BUILD_DIR}/_build/src/${PKG}
CMDS:=$(shell cd cmd && ls)
TEST_REPO=internal/testhelper/testdata/data/gitlab-test.git
VERSION=$(shell git describe)-$(shell date -u +%Y%m%d.%H%M%S)

export GOPATH=${BUILD_DIR}/_build
export GO15VENDOREXPERIMENT=1
export PATH:=${GOPATH}/bin:$(PATH)

.PHONY: all
all: build

.PHONY: ${BUILD_DIR}/_build
${BUILD_DIR}/_build:
	mkdir -p $@/src/${PKG}
	tar -cf - --exclude _build --exclude .git . | (cd $@/src/${PKG} && tar -xf -)
	touch $@

build:	clean-build ${BUILD_DIR}/_build $(shell find . -name '*.go' -not -path './vendor/*')
	rm -f -- "${BIN_BUILD_DIR}/*"
	go install -ldflags "-X main.version=${VERSION}" ${PKG}/cmd/...
	cp ${BIN_BUILD_DIR}/* ${BUILD_DIR}/

install: build
	mkdir -p $(DESTDIR)${PREFIX}/bin/
	cd ${BIN_BUILD_DIR} && install ${CMDS} ${DESTDIR}${PREFIX}/bin/

verify: lint check-formatting govendor-status

check-formatting: install-developer-tools
	go run _support/gofmt-all.go -n

govendor-status: ${BUILD_DIR}/_build install-developer-tools
	cd ${PKG_BUILD_DIR} && govendor status

${TEST_REPO}:
	git clone --bare https://gitlab.com/gitlab-org/gitlab-test.git $@

test: clean-build ${TEST_REPO} ${BUILD_DIR}/_build
	go test ${PKG}/...

lint: install-developer-tools
	go run _support/lint.go

package: build
	./_support/package/package ${CMDS}

notice:	${BUILD_DIR}/_build install-developer-tools
	rm -f ${PKG_BUILD_DIR}/NOTICE # Avoid NOTICE-in-NOTICE
	cd ${PKG_BUILD_DIR} && govendor license -template _support/notice.template -o ${BUILD_DIR}/NOTICE

notice-up-to-date:	notice
	git ls-files --error-unmatch NOTICE # NOTICE is a tracked file
	git diff --exit-code # there are no changed files

clean:	clean-build
	rm -rf internal/testhelper/testdata
	rm -f $(foreach cmd,${CMDS},./${cmd})

clean-build:
	rm -rf ${BUILD_DIR}/_build

.PHONY: format
format:
	@go run _support/gofmt-all.go -f

.PHONY: install-developer-tools
install-developer-tools:
	@go run _support/go-get-if-missing.go govendor github.com/kardianos/govendor
	@go run _support/go-get-if-missing.go golint github.com/golang/lint/golint
