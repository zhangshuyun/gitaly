package repository

import (
	"bufio"
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetConfig(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	getConfig := func(
		t *testing.T,
		client gitalypb.RepositoryServiceClient,
		repo *gitalypb.Repository,
	) (string, error) {
		ctx, cleanup := testhelper.Context()
		defer cleanup()

		stream, err := client.GetConfig(ctx, &gitalypb.GetConfigRequest{
			Repository: repo,
		})
		require.NoError(t, err)

		reader := streamio.NewReader(func() ([]byte, error) {
			response, err := stream.Recv()
			var bytes []byte
			if response != nil {
				bytes = response.Data
			}
			return bytes, err
		})

		contents, err := ioutil.ReadAll(reader)
		return string(contents), err
	}

	t.Run("normal repo", func(t *testing.T) {
		repo, _, cleanup := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
		defer cleanup()

		config, err := getConfig(t, client, repo)
		require.NoError(t, err)
		require.Equal(t, "[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = true\n", config)
	})

	t.Run("missing config", func(t *testing.T) {
		repo, repoPath, cleanup := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
		defer cleanup()

		configPath := filepath.Join(repoPath, "config")
		require.NoError(t, os.Remove(configPath))

		config, err := getConfig(t, client, repo)
		require.Equal(t, status.Errorf(codes.NotFound, "opening gitconfig: open %s: no such file or directory", configPath), err)
		require.Equal(t, "", config)
	})
}

func TestDeleteConfig(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	testcases := []struct {
		desc    string
		addKeys []string
		reqKeys []string
		code    codes.Code
	}{
		{
			desc: "empty request",
		},
		{
			desc:    "keys that don't exist",
			reqKeys: []string{"test.foo", "test.bar"},
		},
		{
			desc:    "mix of keys that do and do not exist",
			addKeys: []string{"test.bar"},
			reqKeys: []string{"test.foo", "test.bar", "test.baz"},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			repo, repoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
			t.Cleanup(cleanupFn)

			for _, k := range tc.addKeys {
				gittest.Exec(t, cfg, "-C", repoPath, "config", k, "blabla")
			}

			_, err := client.DeleteConfig(ctx, &gitalypb.DeleteConfigRequest{Repository: repo, Keys: tc.reqKeys})
			if tc.code == codes.OK {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.code, status.Code(err), "expected grpc error code")
				return
			}

			actualConfig := gittest.Exec(t, cfg, "-C", repoPath, "config", "-l")
			scanner := bufio.NewScanner(bytes.NewReader(actualConfig))
			for scanner.Scan() {
				for _, k := range tc.reqKeys {
					require.False(t, strings.HasPrefix(scanner.Text(), k+"="), "key %q must not occur in config", k)
				}
			}

			require.NoError(t, scanner.Err())
		})
	}
}

func TestDeleteConfigTransactional(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.TxConfig,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		var votes []voting.Vote
		txManager := transaction.MockManager{
			VoteFn: func(_ context.Context, _ txinfo.Transaction, _ txinfo.PraefectServer, vote voting.Vote) error {
				votes = append(votes, vote)
				return nil
			},
		}

		cfg, repo, repoPath, client := setupRepositoryService(t, testserver.WithTransactionManager(&txManager))

		ctx, err := (&txinfo.PraefectServer{SocketPath: "i-dont-care"}).Inject(ctx)
		require.NoError(t, err)
		ctx, err = txinfo.InjectTransaction(ctx, 1, "node", true)
		require.NoError(t, err)
		ctx = helper.IncomingToOutgoing(ctx)

		unmodifiedContents := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
		gittest.Exec(t, cfg, "-C", repoPath, "config", "delete.me", "now")
		modifiedContents := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))

		_, err = client.DeleteConfig(ctx, &gitalypb.DeleteConfigRequest{
			Repository: repo,
			Keys:       []string{"delete.me"},
		})
		require.NoError(t, err)

		if featureflag.IsEnabled(ctx, featureflag.TxConfig) {
			require.Equal(t, []voting.Vote{
				voting.VoteFromData(modifiedContents),
				voting.VoteFromData(unmodifiedContents),
			}, votes)
		} else {
			require.Len(t, votes, 0)
		}
	})
}

func testSetConfig(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	cfg, _, _, client := setupRepositoryServiceWithRuby(t, cfg, rubySrv)

	testcases := []struct {
		desc     string
		entries  []*gitalypb.SetConfigRequest_Entry
		expected []string
		code     codes.Code
	}{
		{
			desc: "empty request",
		},
		{
			desc: "mix of different types",
			entries: []*gitalypb.SetConfigRequest_Entry{
				&gitalypb.SetConfigRequest_Entry{Key: "test.foo1", Value: &gitalypb.SetConfigRequest_Entry_ValueStr{"hello world"}},
				&gitalypb.SetConfigRequest_Entry{Key: "test.foo2", Value: &gitalypb.SetConfigRequest_Entry_ValueInt32{1234}},
				&gitalypb.SetConfigRequest_Entry{Key: "test.foo3", Value: &gitalypb.SetConfigRequest_Entry_ValueBool{true}},
			},
			expected: []string{
				"test.foo1=hello world",
				"test.foo2=1234",
				"test.foo3=true",
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			testRepo, testRepoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
			defer cleanupFn()

			_, err := client.SetConfig(ctx, &gitalypb.SetConfigRequest{Repository: testRepo, Entries: tc.entries})
			if tc.code == codes.OK {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.code, status.Code(err), "expected grpc error code")
				return
			}

			actualConfigBytes := gittest.Exec(t, cfg, "-C", testRepoPath, "config", "--local", "-l")
			scanner := bufio.NewScanner(bytes.NewReader(actualConfigBytes))

			var actualConfig []string
			for scanner.Scan() {
				actualConfig = append(actualConfig, scanner.Text())
			}
			require.NoError(t, scanner.Err())

			for _, entry := range tc.expected {
				require.Contains(t, actualConfig, entry)
			}
		})
	}
}

func testSetConfigTransactional(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.TxConfig,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		var votes []voting.Vote

		txManager := transaction.MockManager{
			VoteFn: func(_ context.Context, _ txinfo.Transaction, _ txinfo.PraefectServer, vote voting.Vote) error {
				votes = append(votes, vote)
				return nil
			},
		}

		_, repo, repoPath, client := setupRepositoryServiceWithRuby(t, cfg, rubySrv, testserver.WithTransactionManager(&txManager))

		ctx, err := (&txinfo.PraefectServer{SocketPath: "i-dont-care"}).Inject(ctx)
		require.NoError(t, err)
		ctx, err = txinfo.InjectTransaction(ctx, 1, "node", true)
		require.NoError(t, err)
		ctx = helper.IncomingToOutgoing(ctx)

		unmodifiedContents := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))

		_, err = client.SetConfig(ctx, &gitalypb.SetConfigRequest{
			Repository: repo,
			Entries: []*gitalypb.SetConfigRequest_Entry{
				&gitalypb.SetConfigRequest_Entry{
					Key: "set.me",
					Value: &gitalypb.SetConfigRequest_Entry_ValueStr{
						"something",
					},
				},
			},
		})
		require.NoError(t, err)

		if featureflag.IsEnabled(ctx, featureflag.TxConfig) {
			modifiedContents := string(unmodifiedContents) + "[set]\n\tme = something\n"
			require.Equal(t, []voting.Vote{
				voting.VoteFromData(unmodifiedContents),
				voting.VoteFromData([]byte(modifiedContents)),
			}, votes)
		} else {
			require.Len(t, votes, 0)
		}
	})
}
