package supervisor

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetRss(t *testing.T) {
	rss := getRss(os.Getpid())
	require.True(t, rss > 0, "Expected a positive RSS")
}
