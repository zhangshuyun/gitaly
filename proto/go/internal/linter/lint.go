package linter

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"gitlab.com/gitlab-org/gitaly/v14/internal/protoutil"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// ensureMethodOpType will ensure that method includes the op_type option.
// See proto example below:
//
//  rpc ExampleMethod(ExampleMethodRequest) returns (ExampleMethodResponse) {
//     option (op_type).op = ACCESSOR;
//   }
func ensureMethodOpType(fileDesc *descriptor.FileDescriptorProto, m *descriptor.MethodDescriptorProto, req *plugin.CodeGeneratorRequest) error {
	opMsg, err := protoutil.GetOpExtension(m)
	if err != nil {
		if errors.Is(err, proto.ErrMissingExtension) {
			return fmt.Errorf("missing op_type extension")
		}

		return err
	}

	ml := methodLinter{
		req:        req,
		fileDesc:   fileDesc,
		methodDesc: m,
		opMsg:      opMsg,
	}

	switch opCode := opMsg.GetOp(); opCode {

	case gitalypb.OperationMsg_ACCESSOR:
		return ml.validateAccessor()

	case gitalypb.OperationMsg_MUTATOR:
		// if mutator, we need to make sure we specify scope or target repo
		return ml.validateMutator()

	case gitalypb.OperationMsg_UNKNOWN:
		return errors.New("op set to UNKNOWN")

	default:
		return fmt.Errorf("invalid operation class with int32 value of %d", opCode)
	}
}

func validateMethod(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto, method *descriptor.MethodDescriptorProto, req *plugin.CodeGeneratorRequest) error {
	if intercepted, err := protoutil.IsInterceptedService(service); err != nil {
		return fmt.Errorf("is intercepted service: %w", err)
	} else if intercepted {
		if _, err := protoutil.GetOpExtension(method); err != nil {
			if errors.Is(err, proto.ErrMissingExtension) {
				return nil
			}

			return err
		}

		return fmt.Errorf("operation type defined on an intercepted method")
	}

	return ensureMethodOpType(file, method, req)
}

// LintFile ensures the file described meets Gitaly required processes.
// Currently, this is limited to validating if request messages contain
// a mandatory operation code.
func LintFile(file *descriptor.FileDescriptorProto, req *plugin.CodeGeneratorRequest) []error {
	var errs []error

	for _, service := range file.GetService() {
		for _, method := range service.GetMethod() {
			if err := validateMethod(file, service, method, req); err != nil {
				errs = append(errs, formatError(file.GetName(), service.GetName(), method.GetName(), err))
			}
		}
	}

	return errs
}

func formatError(file, service, method string, err error) error {
	return fmt.Errorf("%s: service %q: method: %q: %w", file, service, method, err)
}
