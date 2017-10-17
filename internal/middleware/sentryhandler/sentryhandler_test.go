package sentryhandler

import (
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func Test_generateRavenPacket(t *testing.T) {
	tests := []struct {
		name        string
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
			err:         grpc.Errorf(codes.NotFound, "Something failed"),
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now().Add(-tt.sinceStart)
			packet, tags := generateRavenPacket(context.Background(), tt.method, start, tt.err)

			if tt.wantNil {
				assert.Nil(t, packet)
				return
			}

			assert.Equal(t, tt.wantCulprit, packet.Culprit)
			assert.Equal(t, tt.wantMessage, packet.Message)
			assert.Equal(t, tags["system"], "grpc")
			assert.NotEmpty(t, tags["grpc.time_ms"])
			assert.Equal(t, tt.method, tags["grpc.method"])
			assert.Equal(t, tt.wantCode.String(), tags["grpc.code"])
			assert.Equal(t, []string{"grpc", tt.wantCulprit, tt.wantCode.String()}, packet.Fingerprint)
		})
	}
}
