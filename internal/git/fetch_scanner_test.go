package git

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFetchScannerScan(t *testing.T) {
	blank := FetchStatusLine{}

	for _, tc := range []struct {
		desc         string
		data         string
		expected     FetchStatusLine
		success      bool
		isTagAdded   bool
		isTagUpdated bool
	}{
		{
			desc:     "empty line",
			data:     "",
			expected: blank,
		},
		{
			desc:     "blank line",
			data:     "    ",
			expected: blank,
		},
		{
			desc:     "line with a false-positive type",
			data:     "****",
			expected: blank,
		},
		{
			desc:     "line missing initial whitespace",
			data:     "* [new branch]          foo         -> upstream/foo",
			expected: blank,
		},
		{
			desc:     "summary field with spaces missing closing square bracket",
			data:     " * [new branch          foo     -> upstream/foo",
			expected: blank,
		},
		{
			desc:     "missing delimiter between from and to fields (no reason field)",
			data:     "* [new branch]          foo    upstream/foo",
			expected: blank,
		},
		{
			desc:     "missing delimiter between from and to fields (with reason field)",
			data:     " * [new branch]          foo    upstream/foo (some reason)",
			expected: blank,
		},
		{
			desc:     "invalid type with otherwise OK line",
			data:     " ~ [new branch]          foo         -> upstream/foo",
			expected: blank,
		},

		{
			desc:     "valid fetch line (ASCII reference)",
			data:     " * [new branch]          foo         -> upstream/foo",
			expected: FetchStatusLine{RefUpdateTypeFetched, "[new branch]", "foo", "upstream/foo", ""},
			success:  true,
		},
		{
			desc:     "valid fetch line (UTF-8 reference)",
			data:     " * [new branch]          面         -> upstream/面",
			expected: FetchStatusLine{RefUpdateTypeFetched, "[new branch]", "面", "upstream/面", ""},
			success:  true,
		},
		{
			desc:       "valid fetch line (new tag)",
			data:       " * [new tag]             v13.7.0-rc1                             -> v13.7.0-rc1",
			expected:   FetchStatusLine{RefUpdateTypeFetched, "[new tag]", "v13.7.0-rc1", "v13.7.0-rc1", ""},
			success:    true,
			isTagAdded: true,
		},
		{
			desc:     "valid forced-update line",
			data:     " + d8b96a36c...d2a598d09 cgroups-impl                            -> upstream/cgroups-impl  (forced update)",
			expected: FetchStatusLine{RefUpdateTypeForcedUpdate, "d8b96a36c...d2a598d09", "cgroups-impl", "upstream/cgroups-impl", "(forced update)"},
			success:  true,
		},
		{
			desc:     "valid fast-forward update line",
			data:     "   87daf9d2e..1504b30e1  master                       -> upstream/master",
			expected: FetchStatusLine{RefUpdateTypeFastForwardUpdate, "87daf9d2e..1504b30e1", "master", "upstream/master", ""},
			success:  true,
		},
		{
			desc:     "valid prune line (branch reference)",
			data:     " - [deleted]                 (none)     -> upstream/foo",
			expected: FetchStatusLine{RefUpdateTypePruned, "[deleted]", "(none)", "upstream/foo", ""},
			success:  true,
		},
		{
			desc:     "valid prune line (tag reference)",
			data:     " - [deleted]         (none)     -> v1.2.3",
			expected: FetchStatusLine{RefUpdateTypePruned, "[deleted]", "(none)", "v1.2.3", ""},
			success:  true,
		},
		{
			desc:         "valid tag update line",
			data:         " t [tag update]                 v1.2.3     -> v1.2.3",
			expected:     FetchStatusLine{RefUpdateTypeTagUpdate, "[tag update]", "v1.2.3", "v1.2.3", ""},
			success:      true,
			isTagUpdated: true,
		},
		{
			desc:     "valid update failed line",
			data:     " ! d8b96a36c...d2a598d09                 foo     -> upstream/foo  (update hook failed)",
			expected: FetchStatusLine{RefUpdateTypeUpdateFailed, "d8b96a36c...d2a598d09", "foo", "upstream/foo", "(update hook failed)"},
			success:  true,
		},
		{
			desc:     "valid unchanged line",
			data:     " = [up to date]                 foo     -> upstream/foo",
			expected: FetchStatusLine{RefUpdateTypeUnchanged, "[up to date]", "foo", "upstream/foo", ""},
			success:  true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			scanner := NewFetchScanner(strings.NewReader(tc.data))
			require.Equal(t, tc.success, scanner.Scan())
			require.Equal(t, tc.expected, scanner.StatusLine())
			require.Equal(t, tc.isTagAdded, scanner.StatusLine().IsTagAdded())
			require.Equal(t, tc.isTagUpdated, scanner.StatusLine().IsTagUpdated())
		})
	}
}
