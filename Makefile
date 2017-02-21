PKG=gitlab.com/gitlab-org/gitaly
BUILD_DIR=$(shell pwd)
PKG_BUILD_DIR:=${BUILD_DIR}/_build/src/${PKG}
CMDS:=$(shell cd cmd && ls)

export GOPATH=${BUILD_DIR}/_build
export PATH:=${GOPATH}/bin:$(PATH)

.PHONY: all
all: build

.PHONY: ${BUILD_DIR}/_build
${BUILD_DIR}/_build:
	mkdir -p $@/src/${PKG}
	tar -cf - --exclude _build --exclude .git . | (cd $@/src/${PKG} && tar -xf -)
	touch $@

build:	clean-build ${BUILD_DIR}/_build $(shell find . -name '*.go' -not -path './vendor/*')
	cd ${PKG_BUILD_DIR} && $(foreach cmd,${CMDS},go build ./cmd/${cmd} && ) true
	mv $(foreach cmd,${CMDS},${PKG_BUILD_DIR}/${cmd}) ${BUILD_DIR}/

verify: lint check-formatting govendor-status notice-up-to-date

check-formatting: install-developer-tools
	go run _support/gofmt-all.go -n

govendor-status: ${BUILD_DIR}/_build install-developer-tools
	cd ${PKG_BUILD_DIR} && govendor status

test: clean-build ${BUILD_DIR}/_build verify
	cd ${PKG_BUILD_DIR} && go test ./...

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
	rm -rf client/testdata
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

gitaly.crt:
	umask 077; openssl req -new -subj "/CN=gitaly/" -x509 -days 3650 -newkey rsa:2048 -nodes -keyout "gitaly.key" -out "gitaly.crt"