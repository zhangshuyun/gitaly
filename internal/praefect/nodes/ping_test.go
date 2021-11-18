package nodes

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
)

func TestNewPingSet(t *testing.T) {
	conf := config.Config{
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
	}

	actual := newPingSet(conf, nil, true)
	expected := map[string]*Ping{
		"tcp://example.com": {
			address: "tcp://example.com",
			storages: map[gitalyStorage][]virtualStorage{
				"foo": {"meow"},
				"bar": {"woof"},
			},
			vStorages: map[virtualStorage]struct{}{
				"meow": {},
				"woof": {},
			},
			token: "abc",
			quiet: true,
		},
	}

	require.Equal(t, expected, actual)
}
