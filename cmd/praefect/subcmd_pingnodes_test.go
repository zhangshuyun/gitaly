package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type mockServerService struct {
	gitalypb.UnimplementedServerServiceServer
	serverInfoFunc func(ctx context.Context, r *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error)
}

func (m mockServerService) ServerInfo(ctx context.Context, r *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
	return m.serverInfoFunc(ctx, r)
}

func TestSubCmdDialNodes(t *testing.T) {
	var resp *gitalypb.ServerInfoResponse
	mockSvc := &mockServerService{
		serverInfoFunc: func(_ context.Context, _ *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
			return resp, nil
		},
	}
	ln, clean := listenAndServe(t,
		[]svcRegistrar{
			registerHealthService,
			registerServerService(mockSvc),
		},
	)
	defer clean()

	decorateLogs := func(s []string) []string {
		for i, ss := range s {
			s[i] = fmt.Sprintf("[unix:/%s]: %s\n", ln.Addr(), ss)
		}
		return s
	}

	for _, tt := range []struct {
		name string
		conf config.Config
		resp *gitalypb.ServerInfoResponse
		logs string
	}{
		{
			name: "2 virtuals, 2 storages, 1 node",
			conf: config.Config{
				VirtualStorages: []*config.VirtualStorage{
					{
						Name: "default",
						Nodes: []*config.Node{
							{
								Storage: "1",
								Address: "unix:/" + ln.Addr().String(),
							},
						},
					},
					{
						Name: "storage-1",
						Nodes: []*config.Node{
							{
								Storage: "2",
								Address: "unix:/" + ln.Addr().String(),
							},
						},
					},
				},
			},
			resp: &gitalypb.ServerInfoResponse{
				StorageStatuses: []*gitalypb.ServerInfoResponse_StorageStatus{
					{
						StorageName: "1",
						Readable:    true,
						Writeable:   true,
					},
					{
						StorageName: "2",
						Readable:    true,
						Writeable:   true,
					},
				},
			},
			logs: strings.Join(decorateLogs([]string{
				"dialing...",
				"dialed successfully!",
				"checking health...",
				"SUCCESS: node is healthy!",
				"checking consistency...",
				"SUCCESS: confirmed Gitaly storage \"1\" in virtual storages [default] is served",
				"SUCCESS: confirmed Gitaly storage \"2\" in virtual storages [storage-1] is served",
				"SUCCESS: node configuration is consistent!",
			}), ""),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resp = tt.resp
			tt.conf.SocketPath = ln.Addr().String()

			output := &bytes.Buffer{}
			defer func(w io.Writer, f int) {
				log.SetOutput(w)
				log.SetFlags(f) // reinstate timestamp
			}(log.Writer(), log.Flags())
			log.SetOutput(output)
			log.SetFlags(0) // remove timestamp to make output deterministic

			cmd := dialNodesSubcommand{}
			require.NoError(t, cmd.Exec(nil, tt.conf))

			require.Equal(t, tt.logs, output.String())
		})
	}
}
