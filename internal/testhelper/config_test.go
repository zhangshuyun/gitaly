package testhelper_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

var cfgData = `
[core]
  bare = false
	commitGraph = true
[remote "origin"]
	url = git@gitlab.com:gitlab-org/gitaly.git
	fetch = +refs/heads/*:refs/remotes/origin/*
`

func TestConfigParser(t *testing.T) {
	cfg, err := testhelper.ParseConfig(bytes.NewBuffer([]byte(cfgData)))
	require.NoError(t, err)

	for _, tc := range []struct {
		section     string
		key         string
		expectValue string
	}{
		{"core", "commitGraph", "true"},
		{"core", "bare", "false"},
		{`remote "origin"`, "url", "git@gitlab.com:gitlab-org/gitaly.git"},
		{`remote "origin"`, "fetch", "+refs/heads/*:refs/remotes/origin/*"},
	} {
		actualValue, ok := cfg.GetValue(tc.section, tc.key)
		require.True(t, ok)
		require.Equalf(t,
			tc.expectValue, actualValue,
			"ensuring correct value for %s:%s", tc.section, tc.key,
		)
	}
}
