package git

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFetchScannerScan(t *testing.T) {
	blank := FetchStatusLine{}

	for i, tc := range []struct {
		data     string
		expected FetchStatusLine
		success  bool
	}{
		{"", blank, false},
		{"    ", blank, false},
		{"****", blank, false},
		{"* [new branch]          foo         -> upstream/foo", blank, false},
		{"  * [new branch]          foo         -> upstream/foo", blank, false},
		{" * [new branch          foo     -> upstream/foo", blank, false},
		{" * new branch          foo     -> upstream/foo", blank, false},
		{" * [new branch]          foo    upstream/foo", blank, false},
		{" * [new branch]          foo    upstream/foo (some reason)", blank, false},
		{
			" * [new branch]          foo         -> upstream/foo",
			FetchStatusLine{RefUpdateTypeFetched, "[new branch]", "foo", "upstream/foo", ""},
			true,
		},
		{
			" * [new branch]          面         -> upstream/面",
			FetchStatusLine{RefUpdateTypeFetched, "[new branch]", "面", "upstream/面", ""},
			true,
		},
		{
			" + d8b96a36c...d2a598d09 cgroups-impl                            -> upstream/cgroups-impl  (forced update)",
			FetchStatusLine{RefUpdateTypeForcedUpdate, "d8b96a36c...d2a598d09", "cgroups-impl", "upstream/cgroups-impl", "(forced update)"},
			true,
		},
		{
			" * [new tag]             v13.7.0-rc1                             -> v13.7.0-rc1",
			FetchStatusLine{RefUpdateTypeFetched, "[new tag]", "v13.7.0-rc1", "v13.7.0-rc1", ""},
			true,
		},
		{
			"   87daf9d2e..1504b30e1  master                       -> upstream/master",
			FetchStatusLine{RefUpdateTypeFastForwardUpdate, "87daf9d2e..1504b30e1", "master", "upstream/master", ""},
			true,
		},
		{
			" - [deleted]                 (none)     -> upstream/foo",
			FetchStatusLine{RefUpdateTypePruned, "[deleted]", "(none)", "upstream/foo", ""},
			true,
		},
		{
			" t d8b96a36c...d2a598d09                 v1.2.3     -> v1.2.3",
			FetchStatusLine{RefUpdateTypeTagUpdate, "d8b96a36c...d2a598d09", "v1.2.3", "v1.2.3", ""},
			true,
		},
		{
			" ! d8b96a36c...d2a598d09                 foo     -> upstream/foo  (update hook failed)",
			FetchStatusLine{RefUpdateTypeUpdateFailed, "d8b96a36c...d2a598d09", "foo", "upstream/foo", "(update hook failed)"},
			true,
		},
		{
			" = [up to date]                 foo     -> upstream/foo",
			FetchStatusLine{RefUpdateTypeUnchanged, "[up to date]", "foo", "upstream/foo", ""},
			true,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			// Regular run
			scanner := NewFetchScanner(strings.NewReader(tc.data))
			require.Equal(t, tc.success, scanner.Scan())
			require.Equal(t, tc.expected, scanner.StatusLine())
		})
	}
}
