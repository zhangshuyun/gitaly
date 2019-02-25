# internal/praefect/Makefile
#
# This Makefile is responsible for all code generation in Praefect.

# present working directory
PWD := $(shell pwd)

pb/optype.pb.go: pb/optype.proto
	protoc --go_out=$(GOPATH)/src pb/optype.proto

pb/linter/testdata/%.pb.go: pb/linter/testdata/%.proto pb/optype.pb.go
	protoc --go_out=./pb -I${PWD}/pb -I${PWD}/pb/linter/testdata ${PWD}/pb/linter/testdata/*.proto
