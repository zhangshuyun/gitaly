// +build postgres

package importer

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/internalgitaly"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestRepositoryImporter_Run(t *testing.T) {
	defer glsql.Clean()

	for _, tc := range []struct {
		desc             string
		existingRecords  map[string][]string
		alreadyCompleted map[string]bool
		storages         map[string]map[string][]string
		expectedErrors   map[string]string
		imported         map[string][]string
	}{
		{
			desc: "empty",
			storages: map[string]map[string][]string{
				"virtual-storage": {"primary": {}},
			},
			expectedErrors: map[string]string{},
			imported:       map[string][]string{},
		},
		{
			desc: "single repo imported",
			storages: map[string]map[string][]string{
				"virtual-storage": {
					"primary": {"repository-1"},
				},
			},
			expectedErrors: map[string]string{},
			imported: map[string][]string{
				"virtual-storage": {"repository-1"},
			},
		},
		{
			desc: "nested directories not imported",
			storages: map[string]map[string][]string{
				"virtual-storage": {
					"primary": {"parent-repository", filepath.Join("parent-repository", "nested-repository")},
				},
			},
			expectedErrors: map[string]string{},
			imported: map[string][]string{
				"virtual-storage": {"parent-repository"},
			},
		},
		{
			desc: "multi folder hierarchies imported",
			storages: map[string]map[string][]string{
				"virtual-storage": {
					"primary": {filepath.Join("empty-parent-folder", "repository-1")},
				},
			},
			expectedErrors: map[string]string{},
			imported: map[string][]string{
				"virtual-storage": {filepath.Join("empty-parent-folder", "repository-1")},
			},
		},
		{
			desc: "multiple virtual storages imported",
			alreadyCompleted: map[string]bool{
				"virtual-storage-2": false,
			},
			storages: map[string]map[string][]string{
				"virtual-storage-1": {"primary": {"repository-1"}},
				"virtual-storage-2": {"primary": {"repository-2"}},
			},
			expectedErrors: map[string]string{},
			imported: map[string][]string{
				"virtual-storage-1": {"repository-1"},
				"virtual-storage-2": {"repository-2"},
			},
		},
		{
			desc: "secondaries ignored",
			storages: map[string]map[string][]string{
				"virtual-storage": {
					"primary":   {"repository-1"},
					"secondary": {"repository-2"},
				},
			},
			expectedErrors: map[string]string{},
			imported: map[string][]string{
				"virtual-storage": {"repository-1"},
			},
		},
		{
			desc: "storages bigger than batch size work",
			storages: map[string]map[string][]string{
				"virtual-storage": {
					"primary": func() []string {
						repos := make([]string, 2*batchSize+1)
						for i := range repos {
							repos[i] = fmt.Sprintf("repository-%d", i)
						}
						return repos
					}(),
				},
			},
			expectedErrors: map[string]string{},
			imported: map[string][]string{
				"virtual-storage": func() []string {
					repos := make([]string, 2*batchSize+1)
					for i := range repos {
						repos[i] = fmt.Sprintf("repository-%d", i)
					}
					sort.Strings(repos)
					return repos
				}(),
			},
		},
		{
			desc:             "importing skipped when already perfomed",
			alreadyCompleted: map[string]bool{"virtual-storage": true},
			storages: map[string]map[string][]string{
				"virtual-storage": {
					"primary": {"unimported-repository"},
				},
			},
			expectedErrors: map[string]string{},
			imported:       map[string][]string{},
		},
		{
			desc: "errors dont cancel jobs for other virtual storages",
			storages: map[string]map[string][]string{
				"erroring-virtual-storage": {
					"primary": {"repository-1"},
				},
				"successful-virtual-storage": {
					"primary": {"repository-2"},
				},
			},
			expectedErrors: map[string]string{
				"erroring-virtual-storage": fmt.Sprintf("importing virtual storage: get shard: %v", assert.AnError),
			},
			imported: map[string][]string{
				"successful-virtual-storage": {"repository-2"},
			},
		},
		{
			desc:            "repositories with existing records are ignored",
			existingRecords: map[string][]string{"virtual-storage": {"already-existing"}},
			storages: map[string]map[string][]string{
				"virtual-storage": {"primary": {"already-existing", "imported"}},
			},
			expectedErrors: map[string]string{},
			imported: map[string][]string{
				"virtual-storage": {"imported"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db := glsql.GetDB(t, "importer")

			srv := grpc.NewServer()
			defer srv.Stop()

			tmp, err := ioutil.TempDir("", "praefect-importer")
			require.NoError(t, err)
			defer os.RemoveAll(tmp)

			for virtualStorage, completed := range tc.alreadyCompleted {
				_, err := db.ExecContext(ctx, `
					INSERT INTO virtual_storages (virtual_storage, repositories_imported)
					VALUES ($1, $2)
				`, virtualStorage, completed)
				require.NoError(t, err)
			}

			var configuredStorages []config.Storage

			// storage names are prefixed with the virtual storage to reuse the single gitaly server
			// without directory name collisions
			storageName := func(virtualStorage, storage string) string {
				return fmt.Sprintf("%s-%s", virtualStorage, storage)
			}

			// create the repositories on the storages
			for virtualStorage, storages := range tc.storages {
				for storage, relativePaths := range storages {
					storagePath := filepath.Join(tmp, virtualStorage, storage)
					for _, relativePath := range relativePaths {
						repoPath := filepath.Join(storagePath, relativePath)
						require.NoError(t, os.MkdirAll(repoPath, os.ModePerm))
						// WalkFiles checks these files for determining git repositories, we create them
						// here instead of creating a full repo
						for _, filePath := range []string{"objects", "refs", "HEAD"} {
							require.NoError(t, os.Mkdir(filepath.Join(repoPath, filePath), os.ModePerm))
						}
					}

					configuredStorages = append(configuredStorages, config.Storage{
						Name: storageName(virtualStorage, storage),
						Path: storagePath,
					})
				}
			}

			gitalypb.RegisterInternalGitalyServer(srv, internalgitaly.NewServer(configuredStorages))

			socketPath := filepath.Join(tmp, "socket")

			ln, err := net.Listen("unix", socketPath)
			require.NoError(t, err)
			defer ln.Close()

			go srv.Serve(ln)

			conn, err := client.Dial("unix://"+socketPath, nil)
			require.NoError(t, err)

			virtualStorages := make([]string, 0, len(tc.storages))
			for vs := range tc.storages {
				virtualStorages = append(virtualStorages, vs)
			}

			rs := datastore.NewPostgresRepositoryStore(db, nil)
			for virtualStorage, relativePaths := range tc.existingRecords {
				for _, relativePath := range relativePaths {
					require.NoError(t, rs.SetGeneration(ctx, virtualStorage, relativePath, "any-storage", 0))
				}
			}

			importer := New(&nodes.MockManager{
				GetShardFunc: func(virtualStorage string) (nodes.Shard, error) {
					if msg := tc.expectedErrors[virtualStorage]; msg != "" {
						return nodes.Shard{}, assert.AnError
					}

					return nodes.Shard{
						Primary: &nodes.MockNode{
							GetStorageMethod: func() string { return storageName(virtualStorage, "primary") },
							Conn:             conn,
						},
					}, nil
				},
			}, virtualStorages, db)

			actualErrors := map[string]string{}
			imported := map[string][]string{}
			for result := range importer.Run(ctx) {
				if result.Error != nil {
					actualErrors[result.VirtualStorage] = result.Error.Error()
					continue
				}

				imported[result.VirtualStorage] = append(imported[result.VirtualStorage], result.RelativePaths...)
			}

			require.Equal(t, tc.expectedErrors, actualErrors)
			require.Equal(t, tc.imported, imported)

			for virtualStorage := range tc.storages {
				expectedCompleted := true
				if _, ok := tc.expectedErrors[virtualStorage]; ok {
					expectedCompleted = false
				}

				actualCompleted, err := importer.isAlreadyCompleted(ctx, virtualStorage)
				require.NoError(t, err)
				require.Equal(t, expectedCompleted, actualCompleted, virtualStorage)
			}
		})
	}
}
