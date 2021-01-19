package linter

import (
	"errors"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/proto/go/internal"
	_ "gitlab.com/gitlab-org/gitaly/proto/go/internal/linter/testdata"
)

func TestLintFile(t *testing.T) {
	for _, tt := range []struct {
		protoPath string
		errs      []error
	}{
		{
			protoPath: "go/internal/linter/testdata/valid.proto",
			errs:      nil,
		},
		{
			protoPath: "go/internal/linter/testdata/invalid.proto",
			errs: []error{
				formatError("go/internal/linter/testdata/invalid.proto", "InterceptedWithOperationType", "InvalidMethod", errors.New("operation type defined on an intercepted method")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod0", errors.New("missing op_type extension")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod1", errors.New("op set to UNKNOWN")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod2", errors.New("unexpected count of target_repository fields 0, expected 1, found target_repository label at: []")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod4", errors.New("unexpected count of target_repository fields 0, expected 1, found target_repository label at: []")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod5", errors.New("wrong type of field RequestWithWrongTypeRepository.header.repository, expected .gitaly.Repository, got .test.InvalidMethodResponse")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod6", errors.New("unexpected count of target_repository fields 0, expected 1, found target_repository label at: []")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod7", errors.New("unexpected count of target_repository fields 0, expected 1, found target_repository label at: []")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod8", errors.New("unexpected count of target_repository fields 0, expected 1, found target_repository label at: []")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod9", errors.New("unexpected count of target_repository fields 1, expected 0, found target_repository label at: [InvalidMethodRequestWithRepo.destination]")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod10", errors.New("unexpected count of storage field 1, expected 0, found storage label at: [RequestWithStorageAndRepo.storage_name]")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod11", errors.New("unexpected count of storage field 1, expected 0, found storage label at: [RequestWithNestedStorageAndRepo.inner_message.storage_name]")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod13", errors.New("unexpected count of storage field 0, expected 1, found storage label at: []")),
				formatError("go/internal/linter/testdata/invalid.proto", "InvalidService", "InvalidMethod14", errors.New("unexpected count of storage field 2, expected 1, found storage label at: [RequestWithMultipleNestedStorage.inner_message.storage_name RequestWithMultipleNestedStorage.storage_name]")),
			},
		},
	} {
		t.Run(tt.protoPath, func(t *testing.T) {
			fdToCheck, err := internal.ExtractFile(proto.FileDescriptor(tt.protoPath))
			require.NoError(t, err)

			req := &plugin.CodeGeneratorRequest{
				ProtoFile: []*descriptor.FileDescriptorProto{fdToCheck},
			}

			for _, protoPath := range []string{
				// as we have no input stream we can use to create CodeGeneratorRequest
				// we must create it by hands with all required dependencies loaded
				"google/protobuf/descriptor.proto",
				"google/protobuf/timestamp.proto",
				"lint.proto",
				"shared.proto",
			} {
				fd, err := internal.ExtractFile(proto.FileDescriptor(protoPath))
				require.NoError(t, err)
				req.ProtoFile = append(req.ProtoFile, fd)
			}

			errs := LintFile(fdToCheck, req)
			require.Equal(t, tt.errs, errs)
		})
	}
}
