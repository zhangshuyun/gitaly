package sentryhandler

import (
	"context"
	"fmt"
	"testing"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Test_generateSentryEvent(t *testing.T) {
	tests := []struct {
		name        string
		ctx         context.Context
		method      string
		sinceStart  time.Duration
		wantNil     bool
		err         error
		wantCode    codes.Code
		wantMessage string
		wantCulprit string
	}{
		{
			name:        "internal error",
			method:      "/gitaly.SSHService/SSHUploadPack",
			sinceStart:  500 * time.Millisecond,
			err:         fmt.Errorf("Internal"),
			wantCode:    codes.Unknown,
			wantMessage: "Internal",
			wantCulprit: "SSHService::SSHUploadPack",
		},
		{
			name:        "GRPC error",
			method:      "/gitaly.RepoService/RepoExists",
			sinceStart:  500 * time.Millisecond,
			err:         status.Errorf(codes.NotFound, "Something failed"),
			wantCode:    codes.NotFound,
			wantMessage: "rpc error: code = NotFound desc = Something failed",
			wantCulprit: "RepoService::RepoExists",
		},
		{
			name:       "nil",
			method:     "/gitaly.RepoService/RepoExists",
			sinceStart: 500 * time.Millisecond,
			err:        nil,
			wantNil:    true,
		},
		{
			name:       "Canceled",
			method:     "/gitaly.RepoService/RepoExists",
			sinceStart: 500 * time.Millisecond,
			err:        status.Errorf(codes.Canceled, "Something failed"),
			wantNil:    true,
		},
		{
			name:       "DeadlineExceeded",
			method:     "/gitaly.RepoService/RepoExists",
			sinceStart: 500 * time.Millisecond,
			err:        status.Errorf(codes.DeadlineExceeded, "Something failed"),
			wantNil:    true,
		},
		{
			name:       "FailedPrecondition",
			method:     "/gitaly.RepoService/RepoExists",
			sinceStart: 500 * time.Millisecond,
			err:        status.Errorf(codes.FailedPrecondition, "Something failed"),
			wantNil:    true,
		},
		{
			name: "marked to skip",
			ctx: func() context.Context {
				var result context.Context
				ctx := context.Background()
				// this is the only way how we could populate context with `tags` assembler
				grpc_ctxtags.UnaryServerInterceptor()(ctx, nil, nil, func(ctx context.Context, req interface{}) (interface{}, error) {
					result = ctx
					return nil, nil
				})
				MarkToSkip(result)
				return result
			}(),
			wantNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			if tt.ctx != nil {
				ctx = tt.ctx
			}
			start := time.Now().Add(-tt.sinceStart)
			event := generateSentryEvent(ctx, tt.method, start, tt.err)

			if tt.wantNil {
				assert.Nil(t, event)
				return
			}

			assert.Equal(t, tt.wantCulprit, event.Transaction)
			assert.Equal(t, tt.wantMessage, event.Message)
			assert.Equal(t, event.Tags["system"], "grpc")
			assert.NotEmpty(t, event.Tags["grpc.time_ms"])
			assert.Equal(t, tt.method, event.Tags["grpc.method"])
			assert.Equal(t, tt.wantCode.String(), event.Tags["grpc.code"])
			assert.Equal(t, []string{"grpc", tt.wantCulprit, tt.wantCode.String()}, event.Fingerprint)
		})
	}
}
