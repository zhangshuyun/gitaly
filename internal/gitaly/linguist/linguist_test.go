package linguist

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func TestInstance_Stats_unmarshalJSONError(t *testing.T) {
	cfg := testcfg.Build(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	ling, err := New(cfg)
	require.NoError(t, err)

	// When an error occurs, this used to trigger JSON marshelling of a plain string
	// the new behaviour shouldn't do that, and return an command error
	_, err = ling.Stats(ctx, cfg, "/var/empty", "deadbeef")
	require.Error(t, err)

	_, ok := err.(*json.SyntaxError)
	require.False(t, ok, "expected the error not be a json Syntax Error")
}

func TestNew(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithRealLinguist())

	ling, err := New(cfg)
	require.NoError(t, err)

	require.Equal(t, "#701516", ling.Color("Ruby"), "color value for 'Ruby'")
}

func TestNew_loadLanguagesCustomPath(t *testing.T) {
	jsonPath, err := filepath.Abs("testdata/fake-languages.json")
	require.NoError(t, err)

	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{Ruby: config.Ruby{LinguistLanguagesPath: jsonPath}}))

	ling, err := New(cfg)
	require.NoError(t, err)

	require.Equal(t, "foo color", ling.Color("FooBar"))
}
