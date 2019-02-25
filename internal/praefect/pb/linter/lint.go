package main

import (
	"fmt"
	"regexp"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/pb"
)

var (
	requestRegex = regexp.MustCompile(".*Request")
)

// ensureMsgOpType will ensure that message includes the op_type option.
// See proto example below:
//
//   message ExampleRequest {
//     option (op_type).op = ACCESSOR;
//   }
func ensureMsgOpType(file string, msg *descriptor.DescriptorProto) error {
	options := msg.GetOptions()
	//
	// if options == nil {
	// 	log.Printf("%s: Message %s missing options", file, msg.GetName())
	// 	return errMissingOpType
	// }

	if !proto.HasExtension(options, pb.E_OpType) {
		return fmt.Errorf(
			"%s: Message %s missing op_type option",
			file,
			msg.GetName(),
		)
	}

	ext, err := proto.GetExtension(options, pb.E_OpType)
	if err != nil {
		return err
	}

	opMsg, ok := ext.(*pb.OperationMsg)
	if !ok {
		return fmt.Errorf("unable to obtain OperationMsg from %#v", ext)
	}

	// TODO: check if enum is set to UNKNOWN:
	switch opMsg.GetOp() {

	case pb.OperationMsg_ACCESSOR, pb.OperationMsg_MUTATOR:
		return nil

	case pb.OperationMsg_UNKNOWN:
		return fmt.Errorf(
			"%s: Message %s has op set to UNKNOWN",
			file,
			msg.GetName(),
		)
	}

	return nil
}

func LintFile(file *descriptor.FileDescriptorProto) []error {
	var errs []error

	for _, msg := range file.GetMessageType() {
		if !requestRegex.MatchString(msg.GetName()) {
			continue
		}

		err := ensureMsgOpType(file.GetName(), msg)
		if err != nil {
			errs = append(errs, err)
		}

	}

	return errs
}
