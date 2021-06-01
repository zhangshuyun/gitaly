package metadatahandler

import (
	"context"
	"fmt"
	"testing"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/metadata"
)

const (
	correlationID = "CORRELATION_ID"
	clientName    = "CLIENT_NAME"
)

func TestAddMetadataTags(t *testing.T) {
	baseContext, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc             string
		metadata         metadata.MD
		deadline         bool
		expectedMetatags metadataTags
	}{
		{
			desc:     "empty metadata",
			metadata: metadata.Pairs(),
			deadline: false,
			expectedMetatags: metadataTags{
				clientName:   unknownValue,
				callSite:     unknownValue,
				authVersion:  unknownValue,
				deadlineType: "none",
			},
		},
		{
			desc:     "context containing metadata",
			metadata: metadata.Pairs("call_site", "testsite"),
			deadline: false,
			expectedMetatags: metadataTags{
				clientName:   unknownValue,
				callSite:     "testsite",
				authVersion:  unknownValue,
				deadlineType: "none",
			},
		},
		{
			desc:     "context containing metadata and a deadline",
			metadata: metadata.Pairs("call_site", "testsite"),
			deadline: true,
			expectedMetatags: metadataTags{
				clientName:   unknownValue,
				callSite:     "testsite",
				authVersion:  unknownValue,
				deadlineType: unknownValue,
			},
		},
		{
			desc:     "context containing metadata and a deadline type",
			metadata: metadata.Pairs("deadline_type", "regular"),
			deadline: true,
			expectedMetatags: metadataTags{
				clientName:   unknownValue,
				callSite:     unknownValue,
				authVersion:  unknownValue,
				deadlineType: "regular",
			},
		},
		{
			desc:     "a context without deadline but with deadline type",
			metadata: metadata.Pairs("deadline_type", "regular"),
			deadline: false,
			expectedMetatags: metadataTags{
				clientName:   unknownValue,
				callSite:     unknownValue,
				authVersion:  unknownValue,
				deadlineType: "none",
			},
		},
		{
			desc:     "with a context containing metadata",
			metadata: metadata.Pairs("deadline_type", "regular", "client_name", "rails"),
			deadline: true,
			expectedMetatags: metadataTags{
				clientName:   "rails",
				callSite:     unknownValue,
				authVersion:  unknownValue,
				deadlineType: "regular",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(baseContext, testCase.metadata)
			if testCase.deadline {
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(50*time.Millisecond))
				defer cancel()
			}
			require.Equal(t, testCase.expectedMetatags, addMetadataTags(ctx))
		})
	}
}

func verifyHandler(ctx context.Context, req interface{}) (interface{}, error) {
	require, ok := req.(*require.Assertions)
	if !ok {
		return nil, fmt.Errorf("unexpected type conversion failure")
	}
	metaTags := addMetadataTags(ctx)
	require.Equal(clientName, metaTags.clientName)

	tags := grpc_ctxtags.Extract(ctx)
	require.True(tags.Has(CorrelationIDKey))
	require.True(tags.Has(ClientNameKey))
	values := tags.Values()
	require.Equal(correlationID, values[CorrelationIDKey])
	require.Equal(clientName, values[ClientNameKey])

	return nil, nil
}

func TestGRPCTags(t *testing.T) {
	require := require.New(t)

	ctx := metadata.NewIncomingContext(
		correlation.ContextWithCorrelation(
			correlation.ContextWithClientName(
				context.Background(),
				clientName,
			),
			correlationID,
		),
		metadata.Pairs(),
	)

	interceptor := grpc_ctxtags.UnaryServerInterceptor()

	_, err := interceptor(ctx, require, nil, verifyHandler)
	require.NoError(err)
}

func Test_extractServiceName(t *testing.T) {
	tests := []struct {
		name           string
		fullMethodName string
		want           string
	}{
		{
			name:           "blank",
			fullMethodName: "",
			want:           unknownValue,
		}, {
			name:           "normal",
			fullMethodName: "/gitaly.OperationService/method",
			want:           "gitaly.OperationService",
		}, {
			name:           "malformed",
			fullMethodName: "//method",
			want:           "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractServiceName(tt.fullMethodName); got != tt.want {
				t.Errorf("extractServiceName() = %v, want %v", got, tt.want)
			}
		})
	}
}
