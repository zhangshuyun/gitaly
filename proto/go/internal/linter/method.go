package linter

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"gitlab.com/gitlab-org/gitaly/v14/internal/protoutil"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type methodLinter struct {
	req        *plugin.CodeGeneratorRequest
	fileDesc   *descriptor.FileDescriptorProto
	methodDesc *descriptor.MethodDescriptorProto
	opMsg      *gitalypb.OperationMsg
}

// validateAccessor will ensure the accessor method does not specify a target
// repo
func (ml methodLinter) validateAccessor() error {
	switch ml.opMsg.GetScopeLevel() {
	case gitalypb.OperationMsg_REPOSITORY:
		return ml.ensureValidRepoScope()
	case gitalypb.OperationMsg_STORAGE:
		return ml.ensureValidStorageScope()
	}

	return nil
}

// validateMutator will ensure the following rules:
//  - Mutator RPC's with repository level scope must specify a target repo
//  - Mutator RPC's without target repo must not be scoped at repo level
func (ml methodLinter) validateMutator() error {
	switch scope := ml.opMsg.GetScopeLevel(); scope {

	case gitalypb.OperationMsg_REPOSITORY:
		return ml.ensureValidRepoScope()

	case gitalypb.OperationMsg_STORAGE:
		return ml.ensureValidStorageScope()

	default:
		return fmt.Errorf("unknown operation scope level %d", scope)

	}
}

func (ml methodLinter) ensureValidStorageScope() error {
	if err := ml.ensureValidTargetRepository(0); err != nil {
		return err
	}

	return ml.ensureValidStorage(1)
}

func (ml methodLinter) ensureValidRepoScope() error {
	if err := ml.ensureValidTargetRepository(1); err != nil {
		return err
	}
	return ml.ensureValidStorage(0)
}

func (ml methodLinter) ensureValidStorage(expected int) error {
	topLevelMsgs, err := ml.getTopLevelMsgs()
	if err != nil {
		return err
	}

	reqMsgName, err := lastName(ml.methodDesc.GetInputType())
	if err != nil {
		return err
	}

	msgT := topLevelMsgs[reqMsgName]

	m := matcher{
		match:        protoutil.GetStorageExtension,
		subMatch:     nil,
		expectedType: "",
		topLevelMsgs: topLevelMsgs,
	}

	storageFields, err := m.findMatchingFields(reqMsgName, msgT)
	if err != nil {
		return err
	}

	if len(storageFields) != expected {
		return fmt.Errorf("unexpected count of storage field %d, expected %d, found storage label at: %v", len(storageFields), expected, storageFields)
	}

	return nil
}

func (ml methodLinter) ensureValidTargetRepository(expected int) error {
	topLevelMsgs, err := ml.getTopLevelMsgs()
	if err != nil {
		return err
	}

	reqMsgName, err := lastName(ml.methodDesc.GetInputType())
	if err != nil {
		return err
	}

	msgT := topLevelMsgs[reqMsgName]

	m := matcher{
		match:        protoutil.GetTargetRepositoryExtension,
		subMatch:     protoutil.GetRepositoryExtension,
		expectedType: ".gitaly.Repository",
		topLevelMsgs: topLevelMsgs,
	}

	storageFields, err := m.findMatchingFields(reqMsgName, msgT)
	if err != nil {
		return err
	}

	if len(storageFields) != expected {
		return fmt.Errorf("unexpected count of target_repository fields %d, expected %d, found target_repository label at: %v", len(storageFields), expected, storageFields)
	}

	return nil
}

func (ml methodLinter) getTopLevelMsgs() (map[string]*descriptor.DescriptorProto, error) {
	topLevelMsgs := map[string]*descriptor.DescriptorProto{}

	types, err := getFileTypes(ml.fileDesc.GetName(), ml.req)
	if err != nil {
		return nil, err
	}
	for _, msg := range types {
		topLevelMsgs[msg.GetName()] = msg
	}
	return topLevelMsgs, nil
}

type matcher struct {
	match        func(*descriptor.FieldDescriptorProto) (bool, error)
	subMatch     func(*descriptor.FieldDescriptorProto) (bool, error)
	expectedType string
	topLevelMsgs map[string]*descriptor.DescriptorProto
}

func (m matcher) findMatchingFields(prefix string, t *descriptor.DescriptorProto) ([]string, error) {
	var storageFields []string
	for _, f := range t.GetField() {
		subMatcher := m
		fullName := prefix + "." + f.GetName()

		match, err := m.match(f)
		if err != nil {
			return nil, err
		}

		if match {
			if f.GetTypeName() == m.expectedType {
				storageFields = append(storageFields, fullName)
				continue
			} else if m.subMatch == nil {
				return nil, fmt.Errorf("wrong type of field %s, expected %s, got %s", fullName, m.expectedType, f.GetTypeName())
			} else {
				subMatcher.match = m.subMatch
				subMatcher.subMatch = nil
			}
		}

		childMsg, err := findChildMsg(m.topLevelMsgs, t, f)
		if err != nil {
			return nil, err
		}

		if childMsg != nil {
			nestedStorageFields, err := subMatcher.findMatchingFields(fullName, childMsg)
			if err != nil {
				return nil, err
			}
			storageFields = append(storageFields, nestedStorageFields...)
		}

	}
	return storageFields, nil
}

func findChildMsg(topLevelMsgs map[string]*descriptor.DescriptorProto, t *descriptor.DescriptorProto, f *descriptor.FieldDescriptorProto) (*descriptor.DescriptorProto, error) {
	var childType *descriptor.DescriptorProto
	const msgPrimitive = "TYPE_MESSAGE"
	if primitive := f.GetType().String(); primitive != msgPrimitive {
		return nil, nil
	}

	msgName, err := lastName(f.GetTypeName())
	if err != nil {
		return nil, err
	}

	for _, nestedType := range t.GetNestedType() {
		if msgName == nestedType.GetName() {
			return nestedType, nil
		}
	}

	if childType = topLevelMsgs[msgName]; childType != nil {
		return childType, nil
	}

	return nil, fmt.Errorf("could not find message type %q", msgName)
}

func getFileTypes(filename string, req *plugin.CodeGeneratorRequest) ([]*descriptor.DescriptorProto, error) {
	var types []*descriptor.DescriptorProto
	var protoFile *descriptor.FileDescriptorProto
	for _, pf := range req.ProtoFile {
		if pf.Name != nil && *pf.Name == filename {
			types = pf.GetMessageType()
			protoFile = pf
			break
		}
	}

	if protoFile == nil {
		return nil, errors.New("proto file could not be found: " + filename)
	}

	for _, dep := range protoFile.Dependency {
		depTypes, err := getFileTypes(dep, req)
		if err != nil {
			return nil, err
		}
		types = append(types, depTypes...)
	}

	return types, nil
}

func lastName(inputType string) (string, error) {
	tokens := strings.Split(inputType, ".")

	msgName := tokens[len(tokens)-1]
	if msgName == "" {
		return "", fmt.Errorf("unable to parse method input type: %s", inputType)
	}

	return msgName, nil
}
