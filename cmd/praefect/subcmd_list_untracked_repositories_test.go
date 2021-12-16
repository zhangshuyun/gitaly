package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestListUntrackedRepositories_FlagSet(t *testing.T) {
	t.Parallel()
	cmd := &listUntrackedRepositories{}
	for _, tc := range []struct {
		desc string
		args []string
		exp  []interface{}
	}{
		{
			desc: "custom value",
			args: []string{"--delimiter", ","},
			exp:  []interface{}{","},
		},
		{
			desc: "default value",
			args: nil,
			exp:  []interface{}{"\n"},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			fs := cmd.FlagSet()
			require.NoError(t, fs.Parse(tc.args))
			require.ElementsMatch(t, tc.exp, []interface{}{cmd.delimiter})
		})
	}
}

func TestListUntrackedRepositories_Exec(t *testing.T) {
	t.Parallel()
	g1Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-1"))
	g2Cfg := testcfg.Build(t, testcfg.WithStorages("gitaly-2"))

	// Repositories not managed by praefect.
	repo1, _ := gittest.InitRepo(t, g1Cfg, g1Cfg.Storages[0])
	repo2, _ := gittest.InitRepo(t, g1Cfg, g1Cfg.Storages[0])
	repo3, _ := gittest.InitRepo(t, g2Cfg, g2Cfg.Storages[0])

	g1Addr := testserver.RunGitalyServer(t, g1Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	g2Addr := testserver.RunGitalyServer(t, g2Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	db := testdb.New(t)
	var database string
	require.NoError(t, db.QueryRow(`SELECT current_database()`).Scan(&database))
	dbConf := testdb.GetConfig(t, database)

	conf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{Storage: g1Cfg.Storages[0].Name, Address: g1Addr},
					{Storage: g2Cfg.Storages[0].Name, Address: g2Addr},
				},
			},
		},
		DB: dbConf,
	}

	starterConfigs, err := getStarterConfigs(conf)
	require.NoError(t, err)
	stopped := make(chan struct{})
	bootstrapper := bootstrap.NewNoop()
	go func() {
		defer close(stopped)
		assert.NoError(t, run(starterConfigs, conf, bootstrapper, prometheus.NewRegistry(), prometheus.NewRegistry()))
	}()

	cc, err := client.Dial("unix://"+conf.SocketPath, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, cc.Close()) }()
	repoClient := gitalypb.NewRepositoryServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

	praefectStorage := conf.VirtualStorages[0].Name

	// Repository managed by praefect, exists on gitaly-1 and gitaly-2.
	createRepo(t, ctx, repoClient, praefectStorage, "path/to/test/repo")
	out := &bytes.Buffer{}
	cmd := newListUntrackedRepositories(testhelper.NewDiscardingLogger(t), out)
	require.NoError(t, cmd.Exec(flag.NewFlagSet("", flag.PanicOnError), conf))

	exp := []string{
		"The following repositories were found on disk, but missing from the tracking database:",
		fmt.Sprintf(`{"relative_path":%q,"storage":"gitaly-1","virtual_storage":"praefect"}`, repo1.RelativePath),
		fmt.Sprintf(`{"relative_path":%q,"storage":"gitaly-1","virtual_storage":"praefect"}`, repo2.RelativePath),
		fmt.Sprintf(`{"relative_path":%q,"storage":"gitaly-2","virtual_storage":"praefect"}`, repo3.RelativePath),
		"", // an empty extra element required as each line ends with "delimiter" and strings.Split returns all parts
	}
	require.ElementsMatch(t, exp, strings.Split(out.String(), "\n"))

	bootstrapper.Terminate()
	<-stopped
}

func createRepo(t *testing.T, ctx context.Context, repoClient gitalypb.RepositoryServiceClient, storageName, relativePath string) *gitalypb.Repository {
	t.Helper()
	repo := &gitalypb.Repository{
		StorageName:  storageName,
		RelativePath: relativePath,
	}
	for i := 0; true; i++ {
		_, err := repoClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repo})
		if err != nil {
			require.Regexp(t, "(no healthy nodes)|(no such file or directory)|(connection refused)", err.Error())
			require.Less(t, i, 100, "praefect doesn't serve for too long")
			time.Sleep(50 * time.Millisecond)
		} else {
			break
		}
	}
	return repo
}
