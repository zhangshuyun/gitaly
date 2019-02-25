// Command protoc-gen-gitaly is designed to be used as a protobuf compiler
// plugin to verify Gitaly processes are being followed when writing RPC's.
//
// Prerequisites
//
// Protobuf compiler:
// https://github.com/protocolbuffers/protobuf/releases
//
// Usage
//
// To try out, run the following command while in the project root:
//
//   protoc --gitaly_out=. ./internal/praefect/pb/linter/testdata/incomplete.proto
//
// You should see some errors printed to screen for improperly written
// RPC's in the testdata/test.proto file.
package main

import (
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/golang/protobuf/proto"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/pb/linter"
)

func main() {
	data, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("reading input: %s", err)
	}

	req := new(plugin.CodeGeneratorRequest)

	if err := proto.Unmarshal(data, req); err != nil {
		log.Fatalf("parsing input proto: %s", err)
	}

	var errMsgs []string

	// lint each requested file
	for _, pf := range req.GetProtoFile() {
		errs := linter.LintFile(pf)
		for _, err := range errs {
			errMsgs = append(errMsgs, err.Error())
		}
	}

	resp := new(plugin.CodeGeneratorResponse)

	if len(errMsgs) > 0 {
		errMsg := strings.Join(errMsgs, "\n\t")
		resp.Error = &errMsg
	}

	// Send back the results.
	data, err = proto.Marshal(resp)
	if err != nil {
		log.Fatalf("failed to marshal output proto: %s", err)
	}

	_, err = os.Stdout.Write(data)
	if err != nil {
		log.Fatalf("failed to write output proto: %s", err)
	}
}
