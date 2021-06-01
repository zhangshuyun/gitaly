package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"google.golang.org/grpc"
)

func TestRun(t *testing.T) {
	var successPacker packFn = func(_ context.Context, _ *grpc.ClientConn, _ string) (int32, error) { return 0, nil }
	var exitCodePacker packFn = func(_ context.Context, _ *grpc.ClientConn, _ string) (int32, error) { return 123, nil }
	var errorPacker packFn = func(_ context.Context, _ *grpc.ClientConn, _ string) (int32, error) { return 1, fmt.Errorf("fail") }

	gitalyTCPAddress := "tcp://localhost:9999"
	gitalyUnixAddress := fmt.Sprintf("unix://%s", testhelper.GetTemporaryGitalySocketFileName(t))

	tests := []struct {
		name          string
		workingDir    string
		gitalyAddress string
		packer        packFn
		wantCode      int
		wantErr       bool
	}{
		{
			name:          "trivial_tcp",
			packer:        successPacker,
			gitalyAddress: gitalyTCPAddress,
			wantCode:      0,
			wantErr:       false,
		},
		{
			name:          "trivial_unix",
			packer:        successPacker,
			gitalyAddress: gitalyUnixAddress,
			wantCode:      0,
			wantErr:       false,
		},
		{
			name:          "with_working_dir",
			workingDir:    os.TempDir(),
			gitalyAddress: gitalyTCPAddress,
			packer:        successPacker,
			wantCode:      0,
			wantErr:       false,
		},
		{
			name:          "incorrect_working_dir",
			workingDir:    "directory_does_not_exist",
			gitalyAddress: gitalyTCPAddress,
			packer:        successPacker,
			wantCode:      1,
			wantErr:       true,
		},
		{
			name:          "empty_gitaly_address",
			gitalyAddress: "",
			packer:        successPacker,
			wantCode:      1,
			wantErr:       true,
		},
		{
			name:          "invalid_gitaly_address",
			gitalyAddress: "invalid_gitaly_address",
			packer:        successPacker,
			wantCode:      1,
			wantErr:       true,
		},
		{
			name:          "exit_code",
			gitalyAddress: gitalyTCPAddress,
			packer:        exitCodePacker,
			wantCode:      123,
			wantErr:       false,
		},
		{
			name:          "error",
			gitalyAddress: gitalyTCPAddress,
			packer:        errorPacker,
			wantCode:      1,
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := gitalySSHCommand{
				packer:     tt.packer,
				workingDir: tt.workingDir,
				address:    tt.gitalyAddress,
				payload:    "{}",
			}

			gotCode, err := cmd.run()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.wantCode, gotCode)
		})
	}
}
