package protoregistry

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
)

// GitalyProtoFileDescriptors is a slice of all gitaly registered file descriptors
var GitalyProtoFileDescriptors []*descriptor.FileDescriptorProto

func init() {
	for _, protoName := range gitalypb.GitalyProtos {
		gz := proto.FileDescriptor(protoName)
		fd, err := extractFile(gz)
		if err != nil {
			panic(err)
		}

		GitalyProtoFileDescriptors = append(GitalyProtoFileDescriptors, fd)
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

// MethodInfo contains metadata about the RPC method. Refer to documentation
// for message type "OperationMsg" shared.proto in gitlab-org/gitaly-proto for
// more documentation.
type MethodInfo struct {
	Operation   OpType
	targetRepo  []int
	requestName string // protobuf message name for input type
}

// TargetRepo returns the target repository for a protobuf message if it exists
func (mi MethodInfo) TargetRepo(msg proto.Message) (*gitalypb.Repository, error) {
	if mi.requestName != proto.MessageName(msg) {
		return nil, fmt.Errorf(
			"proto message %s does not match expected RPC request message %s",
			proto.MessageName(msg), mi.requestName,
		)
	}

	return reflectFindRepoTarget(msg, mi.targetRepo)
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

	targetRepo, err := parseOID(opMsg.GetTargetRepositoryField())
	if err != nil {
		return MethodInfo{}, err
	}

	// for some reason, the protobuf descriptor contains an extra dot in front
	// of the request name that the generated code does not. This trimming keeps
	// the two copies consistent for comparisons.
	requestName := strings.TrimLeft(methodDesc.GetInputType(), ".")

	return MethodInfo{
		Operation:   opCode,
		targetRepo:  targetRepo,
		requestName: requestName,
	}, nil
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
		return MethodInfo{}, fmt.Errorf("Full method name not found: %v", fullMethodName)
	}
	return methodInfo, nil
}

// extractFile extracts a FileDescriptorProto from a gzip'd buffer.
// https://github.com/golang/protobuf/blob/9eb2c01ac278a5d89ce4b2be68fe4500955d8179/descriptor/descriptor.go#L50
func extractFile(gz []byte) (*descriptor.FileDescriptorProto, error) {
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
