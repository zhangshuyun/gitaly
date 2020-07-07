package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type mockPraefectInfoService struct {
	gitalypb.UnimplementedPraefectInfoServiceServer
	DatalossCheckFunc func(context.Context, *gitalypb.DatalossCheckRequest) (*gitalypb.DatalossCheckResponse, error)
	EnableWritesFunc  func(context.Context, *gitalypb.EnableWritesRequest) (*gitalypb.EnableWritesResponse, error)
}

func (m mockPraefectInfoService) DatalossCheck(ctx context.Context, r *gitalypb.DatalossCheckRequest) (*gitalypb.DatalossCheckResponse, error) {
	return m.DatalossCheckFunc(ctx, r)
}

func (m mockPraefectInfoService) EnableWrites(ctx context.Context, r *gitalypb.EnableWritesRequest) (*gitalypb.EnableWritesResponse, error) {
	return m.EnableWritesFunc(ctx, r)
}

func TestDatalossSubcommand(t *testing.T) {
	mgr := &nodes.MockManager{
		GetShardFunc: func(vs string) (nodes.Shard, error) {
			var primary string
			switch vs {
			case "virtual-storage-1":
				primary = "gitaly-1"
			case "virtual-storage-2":
				primary = "gitaly-4"
			default:
				t.Error("unexpected virtual storage")
			}

			return nodes.Shard{Primary: &nodes.MockNode{StorageName: primary}}, nil
		},
	}

	gs := datastore.NewLocalGenerationStore(map[string][]string{
		"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
		"virtual-storage-2": {"gitaly-4"},
	})

	ctx, cancel := testhelper.Context()
	defer cancel()

	require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "gitaly-1", "repository-1", 1))
	require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "gitaly-2", "repository-1", 0))

	require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "gitaly-2", "repository-2", 0))
	require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "gitaly-3", "repository-2", 0))

	ln, clean := listenAndServe(t, []svcRegistrar{
		registerPraefectInfoServer(info.NewServer(mgr, config.Config{}, nil, gs))})
	defer clean()
	for _, tc := range []struct {
		desc            string
		args            []string
		virtualStorages []*config.VirtualStorage
		output          string
		error           error
	}{
		{
			desc:  "positional arguments",
			args:  []string{"-virtual-storage=virtual-storage-1", "positional-arg"},
			error: UnexpectedPositionalArgsError{Command: "dataloss"},
		},
		{
			desc: "data loss",
			args: []string{"-virtual-storage=virtual-storage-1"}, output: `Virtual storage: virtual-storage-1
  Primary: gitaly-1
  Outdated repositories:
    repository-1:
      gitaly-2 is behind by 1 generation or less
      gitaly-3 is behind by 2 generations or less
    repository-2:
      gitaly-1 is behind by 1 generation or less
`,
		},
		{
			desc:            "multiple virtual storages",
			virtualStorages: []*config.VirtualStorage{{Name: "virtual-storage-2"}, {Name: "virtual-storage-1"}},
			output: `Virtual storage: virtual-storage-1
  Primary: gitaly-1
  Outdated repositories:
    repository-1:
      gitaly-2 is behind by 1 generation or less
      gitaly-3 is behind by 2 generations or less
    repository-2:
      gitaly-1 is behind by 1 generation or less
Virtual storage: virtual-storage-2
  Primary: gitaly-4
  All repositories are consistent!
`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cmd := newDatalossSubcommand()
			output := &bytes.Buffer{}
			cmd.output = output

			fs := cmd.FlagSet()
			require.NoError(t, fs.Parse(tc.args))
			require.Equal(t, tc.error, cmd.Exec(fs, config.Config{
				VirtualStorages: tc.virtualStorages,
				SocketPath:      ln.Addr().String(),
			}))
			require.Equal(t, tc.output, output.String())
		})
	}
}
