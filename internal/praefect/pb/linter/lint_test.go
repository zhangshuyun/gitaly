package main_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/stretchr/testify/require"
	main "gitlab.com/pokstad1/protoc-gen-gitaly"
	_ "gitlab.com/pokstad1/protoc-gen-gitaly/testdata"
)

func TestLintFile(t *testing.T) {
	for _, tt := range []struct {
		protoPath string
		errs      []error
	}{
		{
			protoPath: "valid.proto",
			errs:      nil,
		},
		{
			protoPath: "invalid.proto",
			errs: []error{
				errors.New("invalid.proto: Message InvalidRequest has op set to UNKNOWN"),
			},
		},
		{
			protoPath: "incomplete.proto",
			errs: []error{
				errors.New("incomplete.proto: Message IncompleteRequest missing op_type option"),
			},
		},
	} {
		fd, err := extractFile(proto.FileDescriptor(tt.protoPath))
		require.NoError(t, err)

		errs := main.LintFile(fd)
		require.Equal(t, tt.errs, errs)
	}
}

// extractFile extracts a FileDescriptorProto from a gzip'd buffer.
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
