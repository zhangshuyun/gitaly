/*Command protoc-gen-gitaly is designed to be used as a protobuf compiler
plugin to verify Gitaly processes are being followed when writing RPC's.

Usage

The protoc-gen-gitaly linter can be chained into any protoc workflow that
requires verification that Gitaly RPC guidelines are followed. Typically
this can be done by adding the following argument to an existing protoc
command:

  --gitaly_out=.

For example, you may add the linter as an argument to the command responsible
for generating Go code:

  protoc --go_out=. --gitaly_out=. *.proto

Or, you can run the Gitaly linter by itself. To try out, run the following
command while in the project root:

  protoc --gitaly_out=. ./internal/praefect/pb/linter/testdata/incomplete.proto

You should see some errors printed to screen for improperly written
RPC's in the testdata/incomplete.proto file.

Prerequisites

The protobuf compiler (protoc) can be obtained from the GitHub page:
https://github.com/protocolbuffers/protobuf/releases

Background

The protobuf compiler accepts plugins to analyze protobuf files and generate
language specific code.

These plugins require the following convention executable naming convention:

  protoc-gen-$NAME

Where $NAME is the plugin name of the compiler desired. The protobuf compiler
will search the PATH until an executable with that name is found for a
desired plugin. For example, the following protoc command:

  protoc --gitaly_out=. *.proto

The above will search the PATH for an executable named protoc-gen-gitaly

The plugin accepts a protobuf message in STDIN that describes the parsed
protobuf files. A response is sent back on STDOUT that contains any errors.
*/
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
