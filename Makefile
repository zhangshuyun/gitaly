PKG=gitlab.com/gitlab-org/git-access-daemon
BUILD_DIR=$(shell pwd)
CLIENT_BIN=git-daemon-client
SERVER_BIN=git-daemon-server
GIT_BINARIES=git git-upload-archive git-upload-pack git-receive-pack

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
	go build -o ${SERVER_BIN} cmd/server/main.go
	go build -o ${CLIENT_BIN} cmd/client/main.go
	for bin in ${GIT_BINARIES}; do \
		cp ${CLIENT_BIN} $$bin; \
	done

test: ${BUILD_DIR}/_build deps
	cd ${BUILD_DIR}/_build/src/${PKG}/server && go test -v
	cd ${BUILD_DIR}/_build/src/${PKG}/client && go test -v

clean:
	rm -rf ${BUILD_DIR}/_build
	rm -rf client/testdata
	[ -f ${CLIENT_BIN} ] && rm ${CLIENT_BIN}
	[ -f ${SERVER_BIN} ] && rm ${SERVER_BIN}
	for bin in ${GIT_BINARIES}; do \
		[ -f  $$bin ] && rm $$bin; \
	done
