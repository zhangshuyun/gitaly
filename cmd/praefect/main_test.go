package main

import (
	"errors"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer func() {
		if err := glsql.Clean(); err != nil {
			logrus.Error(err)
		}
	}()

	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func TestNoConfigFlag(t *testing.T) {
	_, err := initConfig()

	assert.Equal(t, err, errNoConfigFile)
}

func TestFlattenNodes(t *testing.T) {
	for _, tt := range []struct {
		desc   string
		conf   config.Config
		expect map[string]*nodePing
	}{
		{
			desc: "Flatten common address between storages",
			conf: config.Config{
				VirtualStorages: []*config.VirtualStorage{
					{
						Name: "meow",
						Nodes: []*config.Node{
							{
								Storage: "foo",
								Address: "tcp://example.com",
								Token:   "abc",
							},
						},
					},
					{
						Name: "woof",
						Nodes: []*config.Node{
							{
								Storage: "bar",
								Address: "tcp://example.com",
								Token:   "abc",
							},
						},
					},
				},
			},
			expect: map[string]*nodePing{
				"tcp://example.com": &nodePing{
					address: "tcp://example.com",
					storages: map[gitalyStorage][]virtualStorage{
						"foo": []virtualStorage{"meow"},
						"bar": []virtualStorage{"woof"},
					},
					vStorages: map[virtualStorage]struct{}{
						"meow": struct{}{},
						"woof": struct{}{},
					},
					token: "abc",
				},
			},
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			actual := flattenNodes(tt.conf)
			require.Equal(t, tt.expect, actual)
		})
	}
}

func TestGetStarterConfigs(t *testing.T) {
	for _, tc := range []struct {
		desc   string
		conf   config.Config
		exp    []starter.Config
		expErr error
	}{
		{
			desc:   "no addresses",
			expErr: errors.New("no listening addresses were provided, unable to start"),
		},
		{
			desc: "addresses without schema",
			conf: config.Config{
				ListenAddr:    "127.0.0.1:2306",
				TLSListenAddr: "127.0.0.1:2307",
				SocketPath:    "/socket/path",
			},
			exp: []starter.Config{
				{
					Name:              starter.TCP,
					Addr:              "127.0.0.1:2306",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.TLS,
					Addr:              "127.0.0.1:2307",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.Unix,
					Addr:              "/socket/path",
					HandoverOnUpgrade: true,
				},
			},
		},
		{
			desc: "addresses with schema",
			conf: config.Config{
				ListenAddr:    "tcp://127.0.0.1:2306",
				TLSListenAddr: "tls://127.0.0.1:2307",
				SocketPath:    "unix:///socket/path",
			},
			exp: []starter.Config{
				{
					Name:              starter.TCP,
					Addr:              "127.0.0.1:2306",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.TLS,
					Addr:              "127.0.0.1:2307",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.Unix,
					Addr:              "/socket/path",
					HandoverOnUpgrade: true,
				},
			},
		},
		{
			desc: "addresses without schema",
			conf: config.Config{
				ListenAddr:    "127.0.0.1:2306",
				TLSListenAddr: "127.0.0.1:2307",
				SocketPath:    "/socket/path",
			},
			exp: []starter.Config{
				{
					Name:              starter.TCP,
					Addr:              "127.0.0.1:2306",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.TLS,
					Addr:              "127.0.0.1:2307",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.Unix,
					Addr:              "/socket/path",
					HandoverOnUpgrade: true,
				},
			},
		},
		{
			desc: "addresses with/without schema",
			conf: config.Config{
				ListenAddr:    "127.0.0.1:2306",
				TLSListenAddr: "tls://127.0.0.1:2307",
				SocketPath:    "unix:///socket/path",
			},
			exp: []starter.Config{
				{
					Name:              starter.TCP,
					Addr:              "127.0.0.1:2306",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.TLS,
					Addr:              "127.0.0.1:2307",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.Unix,
					Addr:              "/socket/path",
					HandoverOnUpgrade: true,
				},
			},
		},
		{
			desc: "secure and insecure can't be the same",
			conf: config.Config{
				ListenAddr:    "127.0.0.1:2306",
				TLSListenAddr: "127.0.0.1:2306",
			},
			expErr: errors.New(`same address can't be used for different schemas "127.0.0.1:2306"`),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual, err := getStarterConfigs(tc.conf)
			require.Equal(t, tc.expErr, err)
			require.ElementsMatch(t, tc.exp, actual)
		})
	}
}
