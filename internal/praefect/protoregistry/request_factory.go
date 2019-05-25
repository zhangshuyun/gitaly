package protoregistry

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
)

// requestFactory returns a function to allocate a new instance of the request
// message type for an RPC method. This is useful in gRPC components that treat
// messages generically, like a stream interceptor.
func requestFactory(mdp *descriptor.MethodDescriptorProto) (func() (proto.Message, error), error) {
	reqTypeName := strings.TrimPrefix(mdp.GetInputType(), ".") // not sure why this has a leading dot
	reqType := proto.MessageType(reqTypeName)
	if reqType == nil {
		return nil, fmt.Errorf("unable to retrieve protobuf message type for %s", reqTypeName)
	}

	factory := func() (proto.Message, error) {
		newReq := reflect.New(reqType.Elem())
		val, ok := newReq.Interface().(proto.Message)
		if !ok {
			return nil, fmt.Errorf("method request factory does not return proto message: %#v", newReq)
		}
		return val, nil
	}

	// does the factory actually work? kick the tires...
	if _, err := factory(); err != nil {
		return nil, fmt.Errorf("defective factory function: %q", err)
	}

	return factory, nil
}
