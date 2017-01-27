PKG=gitlab.com/gitlab-org/gitaly
BUILD_DIR=$(shell pwd)
PKG_BUILD_DIR:=${BUILD_DIR}/_build/src/${PKG}
DESTDIR = .
CMDS:=$(shell cd cmd && ls)

export GOPATH=${BUILD_DIR}/_build
export PATH:=${GOPATH}/bin:$(PATH)

.PHONY: ${BUILD_DIR}/_build

all: build

${BUILD_DIR}/_build:
	mkdir -p $@/src/${PKG}
	tar -cf - --exclude _build --exclude .git . | (cd $@/src/${PKG} && tar -xf -)
	touch $@

build:	clean-build ${BUILD_DIR}/_build $(shell find . -name '*.go' -not -path './vendor/*')
	cd ${PKG_BUILD_DIR} && $(foreach cmd,${CMDS},go build ./cmd/${cmd} && ) true
	mv $(foreach cmd,${CMDS},${PKG_BUILD_DIR}/${cmd}) ${DESTDIR}/

test: clean-build ${BUILD_DIR}/_build fmt
	cd ${PKG_BUILD_DIR} && go test ./...

.PHONY:	fmt
fmt:
	_support/gofmt-all -n | awk '{ print } END { if (NR > 0) { print "Please run _support/gofmt-all -f"; exit 1 } }'

package:	build
	./_support/package/package $(foreach cmd,${CMDS},${BUILD_DIR}/${cmd})

clean:	clean-build
	rm -rf client/testdata
	rm -f $(foreach cmd,${CMDS},./${cmd})

clean-build:
	rm -rf ${BUILD_DIR}/_build