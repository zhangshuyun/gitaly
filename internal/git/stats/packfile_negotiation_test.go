package stats

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
)

const (
	oid1 = "78fb81a02b03f0013360292ec5106763af32c287"
	oid2 = "0f6394307cd7d4909be96a0c818d8094a4cb0e5b"
)

func requireParses(t *testing.T, reader io.Reader, expected PackfileNegotiation) {
	actual, err := ParsePackfileNegotiation(reader)
	require.NoError(t, err)
	actual.PayloadSize = 0
	actual.Packets = 0

	require.Equal(t, expected, actual)
}

func TestPackNegoWithInvalidPktline(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	// Write string with invalid length
	buf.WriteString("0002xyz")
	gittest.WritePktlineString(t, buf, "done")

	_, err := ParsePackfileNegotiation(buf)
	require.Error(t, err, "invalid pktlines should be rejected")
}

func TestPackNegoWithSingleWant(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants: 1, Caps: []string{"cap"},
	})
}

func TestPackNegoWithMissingCaps(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants: 1,
	})
}

func TestPackNegoWithMissingWant(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "have "+oid2)
	gittest.WritePktlineString(t, buf, "done")

	_, err := ParsePackfileNegotiation(buf)
	require.Error(t, err, "packfile negotiation with missing 'want' is invalid")
}

func TestPackNegoWithHave(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "have "+oid2)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants: 1, Haves: 1, Caps: []string{"cap"},
	})
}

func TestPackNegoWithMultipleHaveRoundds(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "have "+oid2)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "have "+oid1)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants: 1,
		Haves: 2,
		Caps:  []string{"cap"},
	})
}

func TestPackNegoWithMultipleWants(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "want "+oid2)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants: 2, Caps: []string{"cap"},
	})
}

func TestPackNegoWithMultipleCapLines(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap1")
	gittest.WritePktlineString(t, buf, "want "+oid2+" cap2")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	_, err := ParsePackfileNegotiation(buf)
	require.Error(t, err, "multiple capability announcements should fail to parse")
}

func TestPackNegoWithDeepen(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "deepen 1")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:  1,
		Caps:   []string{"cap"},
		Deepen: "deepen 1",
	})
}

func TestPackNegoWithMultipleDeepens(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "deepen 1")
	gittest.WritePktlineString(t, buf, "deepen-not "+oid2)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:  1,
		Caps:   []string{"cap"},
		Deepen: "deepen-not " + oid2,
	})
}

func TestPackNegoWithShallow(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "shallow "+oid1)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:    1,
		Caps:     []string{"cap"},
		Shallows: 1,
	})
}

func TestPackNegoWithMultipleShallows(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "shallow "+oid1)
	gittest.WritePktlineString(t, buf, "shallow "+oid2)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:    1,
		Caps:     []string{"cap"},
		Shallows: 2,
	})
}

func TestPackNegoWithFilter(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "filter blob:none")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:  1,
		Caps:   []string{"cap"},
		Filter: "blob:none",
	})
}

func TestPackNegoWithMultipleFilters(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap")
	gittest.WritePktlineString(t, buf, "filter blob:none")
	gittest.WritePktlineString(t, buf, "filter blob:limit=1m")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:  1,
		Caps:   []string{"cap"},
		Filter: "blob:limit=1m",
	})
}

func TestPackNegoFullBlown(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "want "+oid1+" cap1 cap2")
	gittest.WritePktlineString(t, buf, "want "+oid2)
	gittest.WritePktlineString(t, buf, "shallow "+oid2)
	gittest.WritePktlineString(t, buf, "shallow "+oid1)
	gittest.WritePktlineString(t, buf, "deepen 1")
	gittest.WritePktlineString(t, buf, "filter blob:none")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "have "+oid2)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "have "+oid1)
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, "done")

	requireParses(t, buf, PackfileNegotiation{
		Wants:    2,
		Haves:    2,
		Caps:     []string{"cap1", "cap2"},
		Shallows: 2,
		Deepen:   "deepen 1",
		Filter:   "blob:none",
	})
}
