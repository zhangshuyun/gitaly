PREFIX=/usr/local
PKG=gitlab.com/gitlab-org/gitaly
BUILD_DIR=${CURDIR}
TARGET_DIR=${BUILD_DIR}/_build
BIN_BUILD_DIR=${TARGET_DIR}/bin
PKG_BUILD_DIR=${TARGET_DIR}/src/${PKG}
CMDS:=$(shell cd cmd && ls)
VERSION=$(shell git describe)-$(shell date -u +%Y%m%d.%H%M%S)
export TEST_REPO_LOCATION=${TARGET_DIR}/testdata/data
TEST_REPO=${TEST_REPO_LOCATION}/gitlab-test.git
COVERAGE_DIR=${TARGET_DIR}/cover
TOOLS_DIR=${BUILD_DIR}/_tools

export GOPATH=${TARGET_DIR}
export GO15VENDOREXPERIMENT=1
export PATH:=${GOPATH}/bin:$(PATH)

.PHONY: all
all: verify build test

${TARGET_DIR}/.ok: Makefile ${TOOLS_DIR}/govendor
	rm -rf -- "${TARGET_DIR}"
	mkdir -p "$(dir ${PKG_BUILD_DIR})"
	ln -sf ../../../.. "${PKG_BUILD_DIR}"
	mkdir -p "${BIN_BUILD_DIR}"
	touch -- "${TARGET_DIR}/.ok"

.PHONY: build
build: ${TARGET_DIR}/.ok
	go install -ldflags "-X main.version=${VERSION}" $(foreach cmd,${CMDS},${PKG}/cmd/${cmd})
	cp $(foreach cmd,${CMDS},${BIN_BUILD_DIR}/${cmd}) ${BUILD_DIR}/

.PHONY: install
install: build
	mkdir -p $(DESTDIR)${PREFIX}/bin/
	cd ${BIN_BUILD_DIR} && install ${CMDS} ${DESTDIR}${PREFIX}/bin/

.PHONY: verify
verify: lint check-formatting govendor-status notice-up-to-date

.PHONY: check-formatting
check-formatting:
	go run _support/gofmt-all.go -n

.PHONY: govendor-status
govendor-status: ${TARGET_DIR}/.ok ${TOOLS_DIR}/govendor
	cd ${PKG_BUILD_DIR} && ${TOOLS_DIR}/govendor status

${TEST_REPO}:
	git clone --bare https://gitlab.com/gitlab-org/gitlab-test.git $@

.PHONY: test
test: ${TARGET_DIR}/.ok ${TEST_REPO}
	go test $(allpackages)

.PHONY: test-race
test-race: ${TARGET_DIR}/.ok ${TEST_REPO}
	GODEBUG=cgocheck=2 go test -v -race $(allpackages)

.PHONY: lint
lint: ${TARGET_DIR}/.ok ${TOOLS_DIR}/golint
	${TOOLS_DIR}/golint $(allpackages)

.PHONY: package
package: build
	./_support/package/package ${CMDS}

.PHONY: notice
notice:	${TARGET_DIR}/.ok ${TOOLS_DIR}/govendor
	cd ${PKG_BUILD_DIR} && ${TOOLS_DIR}/govendor license -template _support/notice.template -o ${BUILD_DIR}/NOTICE

.PHONY: notice-up-to-date
notice-up-to-date: ${TARGET_DIR}/.ok ${TOOLS_DIR}/govendor
	cd ${PKG_BUILD_DIR} && ${TOOLS_DIR}/govendor license -template _support/notice.template -o ${TARGET_DIR}/nutd.temp
	diff _build/nutd.temp NOTICE
	rm -f _build/nutd.temp

.PHONY: clean
clean:
	rm -rf -- ${TARGET_DIR}
	rm -f -- $(foreach cmd,${CMDS},./${cmd})

.PHONY: format
format:
	go run _support/gofmt-all.go -f

.PHONY: cover
cover: ${TARGET_DIR}/.ok ${TEST_REPO} ${TOOLS_DIR}/gocovmerge
	@echo "NOTE: make cover does not exit 1 on failure, don't use it to check for tests success!"
	mkdir -p "${COVERAGE_DIR}"
	rm -f ${COVERAGE_DIR}/*.out "${COVERAGE_DIR}/all.merged" "${COVERAGE_DIR}/all.html"
	@for MOD in $(allpackages); do \
		echo go test -coverpkg=`echo $(allpackages)|tr " " ","` \
			-coverprofile=${COVERAGE_DIR}/unit-`echo $$MOD|tr "/" "_"`.out $$MOD; \
		go test -coverpkg=`echo $(allpackages)|tr " " ","` \
			-coverprofile=${COVERAGE_DIR}/unit-`echo $$MOD|tr "/" "_"`.out \
			$$MOD 2>&1 | grep -v "no packages being tested depend on"; \
	done
	${TOOLS_DIR}/gocovmerge ${COVERAGE_DIR}/*.out > "${COVERAGE_DIR}/all.merged"
	go tool cover -html "${COVERAGE_DIR}/all.merged" -o "${COVERAGE_DIR}/all.html"
	@echo ""
	@echo "=====> Total test coverage: <====="
	@echo ""
	@go tool cover -func "${COVERAGE_DIR}/all.merged"

list: ${TARGET_DIR}/.ok
	echo GOPATH IS $GOPATH
	cd "${PKG_BUILD_DIR}" && ${TOOLS_DIR}/govendor list -no-status +local
	@echo $(allpackages)

_allpackages = $(shell cd "${PKG_BUILD_DIR}" && ${TOOLS_DIR}/govendor list -no-status +local)

# memoize allpackages, so that it's executed only once and only if used
allpackages = $(if $(__allpackages),,$(eval __allpackages := $$(_allpackages)))$(__allpackages)

.PHONY: install-developer-tools
install-developer-tools: ${TOOLS_DIR}/govendor ${TOOLS_DIR}/golint ${TOOLS_DIR}/gocovmerge

${TOOLS_DIR}/govendor:
	go get github.com/kardianos/govendor
	mkdir -p ${TOOLS_DIR}
	mv ${BIN_BUILD_DIR}/govendor ${TOOLS_DIR}/

${TOOLS_DIR}/golint:
	go get github.com/golang/lint/golint
	mkdir -p ${TOOLS_DIR}
	mv ${BIN_BUILD_DIR}/golint ${TOOLS_DIR}/

${TOOLS_DIR}/gocovmerge:
	go get github.com/wadey/gocovmerge
	mkdir -p ${TOOLS_DIR}
	mv ${BIN_BUILD_DIR}/gocovmerge ${TOOLS_DIR}/
