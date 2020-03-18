package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepository_Clone(t *testing.T) {
	src := Repository{
		RelativePath: "a/b",
		Primary: Node{
			Storage: "s1",
			Address: "0.0.0.0",
			Token:   "$ecret",
		},
		Replicas: []Node{
			{
				Storage: "s2",
				Address: "0.0.0.1",
				Token:   "$ecret",
			},
			{
				Storage: "s3",
				Address: "0.0.0.2",
				Token:   "$ecret",
			},
		},
	}

	clone := src.Clone()
	require.Equal(t, src, clone)

	clone.Replicas[0].Address = "0.0.0.3"
	require.Equal(t, "0.0.0.1", src.Replicas[0].Address)
}

func TestNode_MarshalJSON(t *testing.T) {
	token := "secretToken"
	node := &Node{
		Storage:        "storage",
		Address:        "address",
		Token:          token,
		DefaultPrimary: true,
	}

	b, err := json.Marshal(node)
	require.NoError(t, err)
	require.NotContains(t, string(b), "token")
	require.NotContains(t, string(b), token)
}
