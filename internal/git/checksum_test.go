package git

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefsAllowed(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		ref   string
		match bool
	}{
		{ref: "1450cd639e0bc6721eb02800169e464f212cde06 foo/bar", match: false},
		{ref: "259a6fba859cc91c54cd86a2cbd4c2f720e3a19d foo/bar:baz", match: false},
		{ref: "d0a293c0ac821fadfdc086fe528f79423004229d refs/foo/bar:baz", match: false},
		{ref: "21751bf5cb2b556543a11018c1f13b35e44a99d7 tags/v0.0.1", match: false},
		{ref: "498214de67004b1da3d820901307bed2a68a8ef6 keep-around/498214de67004b1da3d820901307bed2a68a8ef6", match: false},
		{ref: "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e merge-requests/11/head", match: false},
		{ref: "c347ca2e140aa667b968e51ed0ffe055501fe4f4 environments/3/head", match: false},
		{ref: "4ed78158b5b018c43005cec917129861541e25bc notes/commits", match: false},
		{ref: "9298d46305ee0d3f4ce288370beaa02637584ff2 HEAD", match: true},
		{ref: "0ed8c6c6752e8c6ea63e7b92a517bf5ac1209c80 refs/heads/markdown", match: true},
		{ref: "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0", match: true},
		{ref: "78a30867c755d774340108cdad5f11254818fb0c refs/keep-around/78a30867c755d774340108cdad5f11254818fb0c", match: true},
		{ref: "c347ca2e140aa667b968e51ed0ffe055501fe4f4 refs/merge-requests/10/head", match: true},
		{ref: "c347ca2e140aa667b968e51ed0ffe055501fe4f4 refs/environments/2/head", match: true},
		{ref: "4ed78158b5b018c43005cec917129861541e25bc refs/notes/commits", match: true},
	}

	for _, tc := range testCases {
		t.Run(tc.ref, func(t *testing.T) {
			match := refsAllowed.Match([]byte(tc.ref))
			require.Equal(t, match, tc.match)
		})
	}
}

func TestChecksum(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		desc          string
		references    []Reference
		showRefOutput []byte
		expected      string
		expectedZero  bool
	}{
		{
			desc:         "zero",
			expectedZero: true,
		},
		{
			desc: "single ref",
			references: []Reference{
				NewReference(ReferenceName("refs/heads/master"), "f21f76b804422462e79f9f99d5b14856e130ed71"),
			},
			showRefOutput: []byte(`f21f76b804422462e79f9f99d5b14856e130ed71 refs/heads/master
`),
			expected: "bba299616da21e268890f1691c3f6ea788c6cdc9",
		},
		{
			desc: "cancelled out ref",
			references: []Reference{
				NewReference(ReferenceName("refs/heads/master"), "f21f76b804422462e79f9f99d5b14856e130ed71"),
				NewReference(ReferenceName("refs/heads/feature"), "f21f76b804422462e79f9f99d5b14856e130ed71"),
				NewReference(ReferenceName("refs/heads/feature"), "f21f76b804422462e79f9f99d5b14856e130ed71"),
			},
			showRefOutput: []byte(`f21f76b804422462e79f9f99d5b14856e130ed71 refs/heads/master
f21f76b804422462e79f9f99d5b14856e130ed71 refs/heads/feature
f21f76b804422462e79f9f99d5b14856e130ed71 refs/heads/feature
`),
			expected: "bba299616da21e268890f1691c3f6ea788c6cdc9",
		},
		{
			desc: "not allowed ref name",
			references: []Reference{
				NewReference(ReferenceName("refs/heads/master"), "f21f76b804422462e79f9f99d5b14856e130ed71"),
				NewReference(ReferenceName("foo/bar"), "860de2dde3d6673d4a494d3c2ceeae9786a556ba"),
			},
			showRefOutput: []byte(`f21f76b804422462e79f9f99d5b14856e130ed71 refs/heads/master
860de2dde3d6673d4a494d3c2ceeae9786a556ba foo/bar
`),
			expected: "bba299616da21e268890f1691c3f6ea788c6cdc9",
		},
		{
			desc: "two refs, same commit",
			references: []Reference{
				NewReference(ReferenceName("refs/heads/master"), "f21f76b804422462e79f9f99d5b14856e130ed71"),
				NewReference(ReferenceName("refs/heads/feature"), "f21f76b804422462e79f9f99d5b14856e130ed71"),
			},
			showRefOutput: []byte(`f21f76b804422462e79f9f99d5b14856e130ed71 refs/heads/master
f21f76b804422462e79f9f99d5b14856e130ed71 refs/heads/feature
`),
			expected: "f5c4203236292be5f212710c0b042051ba3e14cf",
		},
		{
			desc: "two refs, different commits",
			references: []Reference{
				NewReference(ReferenceName("refs/heads/master"), "f21f76b804422462e79f9f99d5b14856e130ed71"),
				NewReference(ReferenceName("refs/heads/feature"), "860de2dde3d6673d4a494d3c2ceeae9786a556ba"),
			},
			showRefOutput: []byte(`f21f76b804422462e79f9f99d5b14856e130ed71 refs/heads/master
860de2dde3d6673d4a494d3c2ceeae9786a556ba refs/heads/feature
`),
			expected: "56ca38be577db6bdda47a840d1061ad47abc3619",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Run("Add", func(t *testing.T) {
				var checksum Checksum

				for _, ref := range tc.references {
					checksum.Add(ref)
				}

				require.Equal(t, tc.expectedZero, checksum.IsZero())
				require.Equal(t, tc.expected, hex.EncodeToString(checksum.Bytes()))
			})

			t.Run("AddBytes", func(t *testing.T) {
				var checksum Checksum

				scanner := bufio.NewScanner(bytes.NewReader(tc.showRefOutput))
				for scanner.Scan() {
					checksum.AddBytes(scanner.Bytes())
				}

				require.NoError(t, scanner.Err())
				require.Equal(t, tc.expectedZero, checksum.IsZero())
				require.Equal(t, tc.expected, hex.EncodeToString(checksum.Bytes()))
			})
		})
	}
}
