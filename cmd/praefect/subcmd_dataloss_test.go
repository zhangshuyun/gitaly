package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

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

			return nodes.Shard{Primary: &nodes.MockNode{
				GetStorageMethod: func() string {
					return primary
				},
			}}, nil
		},
	}

	gs := datastore.NewMemoryRepositoryStore(map[string][]string{
		"virtual-storage-1": {"gitaly-1", "gitaly-2", "gitaly-3"},
		"virtual-storage-2": {"gitaly-4"},
	})

	ctx, cancel := testhelper.Context()
	defer cancel()

	require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "repository-1", "gitaly-1", 1))
	require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "repository-1", "gitaly-2", 0))

	require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "repository-2", "gitaly-2", 0))
	require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "repository-2", "gitaly-3", 0))

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
			error: unexpectedPositionalArgsError{Command: "dataloss"},
		},
		{
			desc: "data loss with read-only repositories",
			args: []string{"-virtual-storage=virtual-storage-1"}, output: `Virtual storage: virtual-storage-1
  Primary: gitaly-1
  Outdated repositories:
    repository-2 (read-only):
      gitaly-1 is behind by 1 change or less
`,
		},
		{
			desc: "data loss with partially replicated repositories",
			args: []string{"-virtual-storage=virtual-storage-1", "-partially-replicated"}, output: `Virtual storage: virtual-storage-1
  Primary: gitaly-1
  Outdated repositories:
    repository-1 (writable):
      gitaly-2 is behind by 1 change or less
      gitaly-3 is behind by 2 changes or less
    repository-2 (read-only):
      gitaly-1 is behind by 1 change or less
`,
		},
		{
			desc:            "multiple virtual storages with read-only repositories",
			virtualStorages: []*config.VirtualStorage{{Name: "virtual-storage-2"}, {Name: "virtual-storage-1"}},
			output: `Virtual storage: virtual-storage-1
  Primary: gitaly-1
  Outdated repositories:
    repository-2 (read-only):
      gitaly-1 is behind by 1 change or less
Virtual storage: virtual-storage-2
  Primary: gitaly-4
  All repositories are writable!
`,
		},
		{
			desc:            "multiple virtual storages with partially replicated repositories",
			args:            []string{"-partially-replicated"},
			virtualStorages: []*config.VirtualStorage{{Name: "virtual-storage-2"}, {Name: "virtual-storage-1"}},
			output: `Virtual storage: virtual-storage-1
  Primary: gitaly-1
  Outdated repositories:
    repository-1 (writable):
      gitaly-2 is behind by 1 change or less
      gitaly-3 is behind by 2 changes or less
    repository-2 (read-only):
      gitaly-1 is behind by 1 change or less
Virtual storage: virtual-storage-2
  Primary: gitaly-4
  All repositories are up to date!
`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cmd := newDatalossSubcommand()
			output := &bytes.Buffer{}
			cmd.output = output

			fs := cmd.FlagSet()
			require.NoError(t, fs.Parse(tc.args))
			err := cmd.Exec(fs, config.Config{
				VirtualStorages: tc.virtualStorages,
				SocketPath:      ln.Addr().String(),
			})
			require.Equal(t, tc.error, err, err)
			require.Equal(t, tc.output, output.String())
		})
	}
}
