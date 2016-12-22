PKG=gitlab.com/gitlab-org/gitaly
BUILD_DIR=$(shell pwd)
CLIENT_BIN=gitaly-client
SERVER_BIN=gitaly-server

export GOPATH=${BUILD_DIR}/_build
export PATH:=${GOPATH}/bin:$(PATH)

.PHONY: ${BUILD_DIR}/_build

all: test build

${BUILD_DIR}/_build:
	mkdir -p $@/src/${PKG}
	tar -cf - --exclude _build --exclude .git . | (cd $@/src/${PKG} && tar -xf -)
	touch $@

deps: ${BUILD_DIR}/_build
	(which govendor) || go get -u github.com/kardianos/govendor
	cd ${BUILD_DIR}/_build/src/${PKG} && govendor fetch +out

build: deps
	cd ${BUILD_DIR}/_build/src/${PKG} && go build -o ${SERVER_BIN} cmd/server/main.go
	cd ${BUILD_DIR}/_build/src/${PKG} && go build -o ${CLIENT_BIN} cmd/client/main.go

test: ${BUILD_DIR}/_build deps
	cd ${BUILD_DIR}/_build/src/${PKG}/server && go test -v
	cd ${BUILD_DIR}/_build/src/${PKG}/client && go test -v
	cd ${BUILD_DIR}/_build/src/${PKG}/helper && go test -v
	cd ${BUILD_DIR}/_build/src/${PKG}/caching && go test -v

clean:
	rm -rf ${BUILD_DIR}/_build
	rm -rf client/testdata
	[ -f ${CLIENT_BIN} ] && rm ${CLIENT_BIN}
	[ -f ${SERVER_BIN} ] && rm ${SERVER_BIN}
