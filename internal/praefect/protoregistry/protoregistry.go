package protoregistry

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
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

	GitalyProtoPreregistered = New()
	if err := GitalyProtoPreregistered.RegisterFiles(GitalyProtoFileDescriptors...); err != nil {
		panic(err)
	}
}

// OpType represents the operation type for a RPC method
type OpType int

const (
	// OpUnknown = unknown operation type
	OpUnknown = iota
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
	ScopeRepository = iota
	// ScopeStorage indicates an RPC is scoped to an entire storage location
	ScopeStorage
	// ScopeServer indicates an RPC is scoped to an entire server
	ScopeServer
)

var protoScope = map[gitalypb.OperationMsg_Scope]Scope{
	gitalypb.OperationMsg_SERVER:     ScopeServer,
	gitalypb.OperationMsg_REPOSITORY: ScopeRepository,
	gitalypb.OperationMsg_STORAGE:    ScopeStorage,
}

// MethodInfo contains metadata about the RPC method. Refer to documentation
// for message type "OperationMsg" shared.proto in gitlab-org/gitaly-proto for
// more documentation.
type MethodInfo struct {
	Operation      OpType
	Scope          Scope
	targetField    []int
	requestName    string // protobuf message name for input type
	requestFactory protoFactory
}

// TargetRepo returns the target repository for a protobuf message if it exists
func (mi MethodInfo) TargetRepo(msg proto.Message) (*gitalypb.Repository, error) {
	if mi.requestName != proto.MessageName(msg) {
		return nil, fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	return reflectFindRepoTarget(msg, mi.targetField)
}

// TargetStorage returns the target storage for a protobuf message if it exists
func (mi MethodInfo) TargetStorage(msg proto.Message) (string, error) {
	if mi.Scope != ScopeStorage {
		return "", fmt.Errorf("proto message %s is not storage scoped", proto.MessageName(msg))
	}

	return reflectFindStringTarget(msg, mi.targetField)
}

// UnmarshalRequestProto will unmarshal the bytes into the method's request
// message type
func (mi MethodInfo) UnmarshalRequestProto(b []byte) (proto.Message, error) {
	return mi.requestFactory(b)
}

// Registry contains info about RPC methods
type Registry struct {
	sync.RWMutex
	protos map[string]MethodInfo
}

// New creates a new ProtoRegistry
func New() *Registry {
	return &Registry{
		protos: make(map[string]MethodInfo),
	}
}

// RegisterFiles takes one or more descriptor.FileDescriptorProto and populates
// the registry with its info
func (pr *Registry) RegisterFiles(protos ...*descriptor.FileDescriptorProto) error {
	pr.Lock()
	defer pr.Unlock()

	for _, p := range protos {
		for _, svc := range p.GetService() {
			for _, method := range svc.GetMethod() {
				mi, err := parseMethodInfo(method)
				if err != nil {
					return err
				}

				fullMethodName := fmt.Sprintf(
					"/%s.%s/%s",
					p.GetPackage(), svc.GetName(), method.GetName(),
				)
				pr.protos[fullMethodName] = mi
			}
		}
	}

	return nil
}

func getOpExtension(m *descriptor.MethodDescriptorProto) (*gitalypb.OperationMsg, error) {
	options := m.GetOptions()

	if !proto.HasExtension(options, gitalypb.E_OpType) {
		return nil, fmt.Errorf("method %s missing op_type option", m.GetName())
	}

	ext, err := proto.GetExtension(options, gitalypb.E_OpType)
	if err != nil {
		return nil, fmt.Errorf("unable to get Gitaly custom OpType extension: %s", err)
	}

	opMsg, ok := ext.(*gitalypb.OperationMsg)
	if !ok {
		return nil, fmt.Errorf("unable to obtain OperationMsg from %#v", ext)
	}
	return opMsg, nil
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

func parseMethodInfo(methodDesc *descriptor.MethodDescriptorProto) (MethodInfo, error) {
	opMsg, err := getOpExtension(methodDesc)
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
	}

	switch scope {
	case ScopeRepository:
		targetField, err := parseOID(opMsg.GetTargetRepositoryField())
		if err != nil {
			return MethodInfo{}, err
		}
		mi.targetField = targetField
	case ScopeStorage:
		targetField, err := parseOID(opMsg.GetTargetStorageField())
		if err != nil {
			return MethodInfo{}, err
		}
		mi.targetField = targetField
	}

	return mi, nil
}

// parses a string like "1.1" and returns a slice of ints
func parseOID(rawFieldOID string) ([]int, error) {
	var fieldNos []int

	if rawFieldOID == "" {
		return fieldNos, nil
	}

	fieldNoStrs := strings.Split(rawFieldOID, ".")

	if len(fieldNoStrs) < 1 {
		return nil,
			fmt.Errorf("OID string contains no field numbers: %s", fieldNoStrs)
	}

	fieldNos = make([]int, len(fieldNoStrs))

	for i, fieldNoStr := range fieldNoStrs {
		fieldNo, err := strconv.Atoi(fieldNoStr)
		if err != nil {
			return nil,
				fmt.Errorf(
					"unable to parse target field OID %s: %s",
					rawFieldOID, err,
				)
		}
		if fieldNo == 0 {
			return nil, errors.New("zero is an invalid field number")
		}
		fieldNos[i] = fieldNo
	}

	return fieldNos, nil
}

// LookupMethod looks up an MethodInfo by service and method name
func (pr *Registry) LookupMethod(fullMethodName string) (MethodInfo, error) {
	pr.RLock()
	defer pr.RUnlock()

	methodInfo, ok := pr.protos[fullMethodName]
	if !ok {
		return MethodInfo{}, fmt.Errorf("full method name not found: %v", fullMethodName)
	}
	return methodInfo, nil
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

	fd := new(descriptor.FileDescriptorProto)
	if err := proto.Unmarshal(b, fd); err != nil {
		return nil, fmt.Errorf("malformed FileDescriptorProto: %v", err)
	}

	return fd, nil
}
