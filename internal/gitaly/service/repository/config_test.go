package repository

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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

			repo, repoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
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

			testRepo, testRepoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
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
