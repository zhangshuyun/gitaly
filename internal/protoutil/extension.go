package protoutil

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// GetOpExtension gets the OperationMsg from a method descriptor
func GetOpExtension(m *descriptor.MethodDescriptorProto) (*gitalypb.OperationMsg, error) {
	ext, err := getExtension(m.GetOptions(), gitalypb.E_OpType)
	if err != nil {
		return nil, err
	}

	return ext.(*gitalypb.OperationMsg), nil
}

// IsInterceptedService returns whether the serivce is intercepted by Praefect.
func IsInterceptedService(s *descriptor.ServiceDescriptorProto) (bool, error) {
	return getBoolExtension(s.GetOptions(), gitalypb.E_Intercepted)
}

// GetRepositoryExtension gets the repository extension from a field descriptor
func GetRepositoryExtension(m *descriptor.FieldDescriptorProto) (bool, error) {
	return getBoolExtension(m.GetOptions(), gitalypb.E_Repository)
}

// GetStorageExtension gets the storage extension from a field descriptor
func GetStorageExtension(m *descriptor.FieldDescriptorProto) (bool, error) {
	return getBoolExtension(m.GetOptions(), gitalypb.E_Storage)
}

// GetTargetRepositoryExtension gets the target_repository extension from a field descriptor
func GetTargetRepositoryExtension(m *descriptor.FieldDescriptorProto) (bool, error) {
	return getBoolExtension(m.GetOptions(), gitalypb.E_TargetRepository)
}

// GetAdditionalRepositoryExtension gets the target_repository extension from a field descriptor
func GetAdditionalRepositoryExtension(m *descriptor.FieldDescriptorProto) (bool, error) {
	return getBoolExtension(m.GetOptions(), gitalypb.E_AdditionalRepository)
}

func getBoolExtension(options proto.Message, extension *proto.ExtensionDesc) (bool, error) {
	val, err := getExtension(options, extension)
	if err != nil {
		if errors.Is(err, proto.ErrMissingExtension) {
			return false, nil
		}

		return false, err
	}

	return *val.(*bool), nil
}

func getExtension(options proto.Message, extension *proto.ExtensionDesc) (interface{}, error) {
	if !proto.HasExtension(options, extension) {
		return nil, fmt.Errorf("protoutil.getExtension %q: %w", extension.TypeDescriptor().FullName(), proto.ErrMissingExtension)
	}

	ext, err := proto.GetExtension(options, extension)
	if err != nil {
		return nil, fmt.Errorf("protoutil.getExtension %q: %w", extension.TypeDescriptor().FullName(), err)
	}

	return ext, nil
}
