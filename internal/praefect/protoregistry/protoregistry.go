package protoregistry

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"gitlab.com/gitlab-org/gitaly/internal/protoutil"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// GitalyProtoFileDescriptors is a slice of all gitaly registered file descriptors
var (
	// GitalyProtoFileDescriptors is a slice of all gitaly registered file
	// descriptors
	GitalyProtoFileDescriptors []*descriptor.FileDescriptorProto
	// GitalyProtoPreregistered is a proto registry pre-registered with all
	// GitalyProtoFileDescriptors file descriptor protos
	GitalyProtoPreregistered *Registry
)

func init() {
	for _, protoName := range gitalypb.GitalyProtos {
		gz := proto.FileDescriptor(protoName)
		fd, err := ExtractFileDescriptor(gz)
		if err != nil {
			panic(err)
		}

		GitalyProtoFileDescriptors = append(GitalyProtoFileDescriptors, fd)
	}

	var err error
	GitalyProtoPreregistered, err = New(GitalyProtoFileDescriptors...)
	if err != nil {
		panic(err)
	}
}

// OpType represents the operation type for a RPC method
type OpType int

const (
	// OpUnknown = unknown operation type
	OpUnknown OpType = iota
	// OpAccessor = accessor operation type (ready only)
	OpAccessor
	// OpMutator = mutator operation type (modifies a repository)
	OpMutator
)

// Scope represents the intended scope of an RPC method
type Scope int

const (
	// ScopeUnknown is the default scope until determined otherwise
	ScopeUnknown = iota
	// ScopeRepository indicates an RPC's scope is limited to a repository
	ScopeRepository Scope = iota
	// ScopeStorage indicates an RPC is scoped to an entire storage location
	ScopeStorage
)

func (s Scope) String() string {
	switch s {
	case ScopeStorage:
		return "storage"
	case ScopeRepository:
		return "repository"
	default:
		return fmt.Sprintf("N/A: %d", s)
	}
}

var protoScope = map[gitalypb.OperationMsg_Scope]Scope{
	gitalypb.OperationMsg_REPOSITORY: ScopeRepository,
	gitalypb.OperationMsg_STORAGE:    ScopeStorage,
}

// MethodInfo contains metadata about the RPC method. Refer to documentation
// for message type "OperationMsg" shared.proto in ./proto for
// more documentation.
type MethodInfo struct {
	Operation      OpType
	Scope          Scope
	targetRepo     []int
	additionalRepo []int
	requestName    string // protobuf message name for input type
	requestFactory protoFactory
	storage        []int
	fullMethodName string
}

// TargetRepo returns the target repository for a protobuf message if it exists
func (mi MethodInfo) TargetRepo(msg proto.Message) (*gitalypb.Repository, error) {
	return mi.getRepo(msg, mi.targetRepo)
}

// AdditionalRepo returns the additional repository for a protobuf message that needs a storage rewritten
// if it exists
func (mi MethodInfo) AdditionalRepo(msg proto.Message) (*gitalypb.Repository, bool, error) {
	if mi.additionalRepo == nil {
		return nil, false, nil
	}

	repo, err := mi.getRepo(msg, mi.additionalRepo)

	return repo, true, err
}

func (mi MethodInfo) FullMethodName() string {
	return mi.fullMethodName
}

func (mi MethodInfo) getRepo(msg proto.Message, targetOid []int) (*gitalypb.Repository, error) {
	if mi.requestName != proto.MessageName(msg) {
		return nil, fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	repo, err := reflectFindRepoTarget(msg, targetOid)
	switch {
	case err != nil:
		return nil, err
	case repo == nil:
		// it is possible for the target repo to not be set (especially in our unit
		// tests designed to fail and this should return an error to prevent nil
		// pointer dereferencing
		return nil, ErrTargetRepoMissing
	default:
		return repo, nil
	}
}

// Storage returns the storage name for a protobuf message if it exists
func (mi MethodInfo) Storage(msg proto.Message) (string, error) {
	if mi.requestName != proto.MessageName(msg) {
		return "", fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	return reflectFindStorage(msg, mi.storage)
}

// SetStorage sets the storage name for a protobuf message
func (mi MethodInfo) SetStorage(msg proto.Message, storage string) error {
	if mi.requestName != proto.MessageName(msg) {
		return fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	return reflectSetStorage(msg, mi.storage, storage)
}

// UnmarshalRequestProto will unmarshal the bytes into the method's request
// message type
func (mi MethodInfo) UnmarshalRequestProto(b []byte) (proto.Message, error) {
	return mi.requestFactory(b)
}

// Registry contains info about RPC methods
type Registry struct {
	protos map[string]MethodInfo
	// interceptedMethods contains the set of methods which are intercepted
	// by Praefect instead of proxying.
	interceptedMethods map[string]struct{}
}

// New creates a new ProtoRegistry with info from one or more descriptor.FileDescriptorProto
func New(protos ...*descriptor.FileDescriptorProto) (*Registry, error) {
	methods := make(map[string]MethodInfo)
	interceptedMethods := make(map[string]struct{})

	for _, p := range protos {
		for _, svc := range p.GetService() {
			for _, method := range svc.GetMethod() {
				fullMethodName := fmt.Sprintf("/%s.%s/%s",
					p.GetPackage(), svc.GetName(), method.GetName(),
				)

				if intercepted, err := protoutil.IsInterceptedService(svc); err != nil {
					return nil, fmt.Errorf("is intercepted: %w", err)
				} else if intercepted {
					interceptedMethods[fullMethodName] = struct{}{}
					continue
				}

				mi, err := parseMethodInfo(p, method, fullMethodName)
				if err != nil {
					return nil, err
				}

				methods[fullMethodName] = mi
			}
		}
	}

	return &Registry{
		protos:             methods,
		interceptedMethods: interceptedMethods,
	}, nil
}

type protoFactory func([]byte) (proto.Message, error)

func methodReqFactory(method *descriptor.MethodDescriptorProto) (protoFactory, error) {
	// for some reason, the descriptor prepends a dot not expected in Go
	inputTypeName := strings.TrimPrefix(method.GetInputType(), ".")

	inputType := proto.MessageType(inputTypeName)
	if inputType == nil {
		return nil, fmt.Errorf("no message type found for %s", inputType)
	}

	f := func(buf []byte) (proto.Message, error) {
		v := reflect.New(inputType.Elem())
		pb, ok := v.Interface().(proto.Message)
		if !ok {
			return nil, fmt.Errorf("factory function expected protobuf message but got %T", v.Interface())
		}

		if err := proto.Unmarshal(buf, pb); err != nil {
			return nil, err
		}

		return pb, nil
	}

	return f, nil
}

func parseMethodInfo(
	p *descriptor.FileDescriptorProto,
	methodDesc *descriptor.MethodDescriptorProto,
	fullMethodName string,
) (MethodInfo, error) {
	opMsg, err := protoutil.GetOpExtension(methodDesc)
	if err != nil {
		return MethodInfo{}, err
	}

	var opCode OpType

	switch opMsg.GetOp() {
	case gitalypb.OperationMsg_ACCESSOR:
		opCode = OpAccessor
	case gitalypb.OperationMsg_MUTATOR:
		opCode = OpMutator
	default:
		opCode = OpUnknown
	}

	// for some reason, the protobuf descriptor contains an extra dot in front
	// of the request name that the generated code does not. This trimming keeps
	// the two copies consistent for comparisons.
	requestName := strings.TrimLeft(methodDesc.GetInputType(), ".")

	reqFactory, err := methodReqFactory(methodDesc)
	if err != nil {
		return MethodInfo{}, err
	}

	scope, ok := protoScope[opMsg.GetScopeLevel()]
	if !ok {
		return MethodInfo{}, fmt.Errorf("encountered unknown method scope %d", opMsg.GetScopeLevel())
	}

	mi := MethodInfo{
		Operation:      opCode,
		Scope:          scope,
		requestName:    requestName,
		requestFactory: reqFactory,
		fullMethodName: fullMethodName,
	}

	topLevelMsgs, err := getTopLevelMsgs(p)
	if err != nil {
		return MethodInfo{}, err
	}

	typeName, err := lastName(methodDesc.GetInputType())
	if err != nil {
		return MethodInfo{}, err
	}

	if scope == ScopeRepository {
		m := matcher{
			match:        protoutil.GetTargetRepositoryExtension,
			subMatch:     protoutil.GetRepositoryExtension,
			expectedType: ".gitaly.Repository",
			topLevelMsgs: topLevelMsgs,
		}

		targetRepo, err := m.findField(topLevelMsgs[typeName])
		if err != nil {
			return MethodInfo{}, err
		}
		if targetRepo == nil {
			return MethodInfo{}, fmt.Errorf("unable to find target repository for method: %s", requestName)
		}
		mi.targetRepo = targetRepo

		m.match = protoutil.GetAdditionalRepositoryExtension
		additionalRepo, err := m.findField(topLevelMsgs[typeName])
		if err != nil {
			return MethodInfo{}, err
		}
		mi.additionalRepo = additionalRepo
	} else if scope == ScopeStorage {
		m := matcher{
			match:        protoutil.GetStorageExtension,
			topLevelMsgs: topLevelMsgs,
		}
		storage, err := m.findField(topLevelMsgs[typeName])
		if err != nil {
			return MethodInfo{}, err
		}
		if storage == nil {
			return MethodInfo{}, fmt.Errorf("unable to find storage for method: %s", requestName)
		}
		mi.storage = storage
	}

	return mi, nil
}

func getFileTypes(filename string) ([]*descriptor.DescriptorProto, error) {
	sharedFD, err := ExtractFileDescriptor(proto.FileDescriptor(filename))
	if err != nil {
		return nil, err
	}

	types := sharedFD.GetMessageType()

	for _, dep := range sharedFD.Dependency {
		depTypes, err := getFileTypes(dep)
		if err != nil {
			return nil, err
		}
		types = append(types, depTypes...)
	}

	return types, nil
}

func getTopLevelMsgs(p *descriptor.FileDescriptorProto) (map[string]*descriptor.DescriptorProto, error) {
	topLevelMsgs := map[string]*descriptor.DescriptorProto{}
	types, err := getFileTypes(p.GetName())
	if err != nil {
		return nil, err
	}
	for _, msg := range types {
		topLevelMsgs[msg.GetName()] = msg
	}
	return topLevelMsgs, nil
}

// Matcher helps find field matching credentials. At first match method is used to check fields
// recursively. Then if field matches but type don't match expectedType subMatch method is used
// from this point. This matcher assumes that only one field in the message matches the credentials.
type matcher struct {
	match        func(*descriptor.FieldDescriptorProto) (bool, error)
	subMatch     func(*descriptor.FieldDescriptorProto) (bool, error)
	expectedType string                                 // fully qualified name of expected type e.g. ".gitaly.Repository"
	topLevelMsgs map[string]*descriptor.DescriptorProto // Map of all top level messages in given file and it dependencies. Result of getTopLevelMsgs should be used.
}

func (m matcher) findField(t *descriptor.DescriptorProto) ([]int, error) {
	for _, f := range t.GetField() {
		match, err := m.match(f)
		if err != nil {
			return nil, err
		}
		if match {
			if f.GetTypeName() == m.expectedType {
				return []int{int(f.GetNumber())}, nil
			} else if m.subMatch != nil {
				m.match = m.subMatch
				m.subMatch = nil
			} else {
				return nil, fmt.Errorf("found wrong type, expected: %s, got: %s", m.expectedType, f.GetTypeName())
			}
		}

		childMsg, err := findChildMsg(m.topLevelMsgs, t, f)
		if err != nil {
			return nil, err
		}

		if childMsg != nil {
			nestedField, err := m.findField(childMsg)
			if err != nil {
				return nil, err
			}
			if nestedField != nil {
				return append([]int{int(f.GetNumber())}, nestedField...), nil
			}
		}
	}
	return nil, nil
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

func lastName(inputType string) (string, error) {
	tokens := strings.Split(inputType, ".")

	msgName := tokens[len(tokens)-1]
	if msgName == "" {
		return "", fmt.Errorf("unable to parse method input type: %s", inputType)
	}

	return msgName, nil
}

// LookupMethod looks up an MethodInfo by service and method name
func (pr *Registry) LookupMethod(fullMethodName string) (MethodInfo, error) {
	methodInfo, ok := pr.protos[fullMethodName]
	if !ok {
		return MethodInfo{}, fmt.Errorf("full method name not found: %v", fullMethodName)
	}
	return methodInfo, nil
}

// Methods returns all registered methods
func (pr *Registry) Methods() []MethodInfo {
	methods := make([]MethodInfo, 0, len(pr.protos))
	for _, proto := range pr.protos {
		methods = append(methods, proto)
	}
	return methods
}

// IsInterceptedMethod returns whether Praefect intercepts the method call instead of proxying it.
func (pr *Registry) IsInterceptedMethod(fullMethodName string) bool {
	_, ok := pr.interceptedMethods[fullMethodName]
	return ok
}

// ExtractFileDescriptor extracts a FileDescriptorProto from a gzip'd buffer.
// https://github.com/golang/protobuf/blob/9eb2c01ac278a5d89ce4b2be68fe4500955d8179/descriptor/descriptor.go#L50
func ExtractFileDescriptor(gz []byte) (*descriptor.FileDescriptorProto, error) {
	r, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, fmt.Errorf("failed to open gzip reader: %v", err)
	}
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to uncompress descriptor: %v", err)
	}

	fd := &descriptor.FileDescriptorProto{}
	if err := proto.Unmarshal(b, fd); err != nil {
		return nil, fmt.Errorf("malformed FileDescriptorProto: %v", err)
	}

	return fd, nil
}
