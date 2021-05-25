package stats

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
)

func TestSingleRefParses(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "# service=git-upload-pack\n")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, oid1+" HEAD\x00capability")
	gittest.WritePktlineFlush(t, buf)

	d, err := ParseReferenceDiscovery(buf)
	require.NoError(t, err)
	require.Equal(t, []string{"capability"}, d.Caps)
	require.Equal(t, []Reference{{Oid: oid1, Name: "HEAD"}}, d.Refs)
}

func TestMultipleRefsAndCapsParse(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "# service=git-upload-pack\n")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, oid1+" HEAD\x00first second")
	gittest.WritePktlineString(t, buf, oid2+" refs/heads/master")
	gittest.WritePktlineFlush(t, buf)

	d, err := ParseReferenceDiscovery(buf)
	require.NoError(t, err)
	require.Equal(t, []string{"first", "second"}, d.Caps)
	require.Equal(t, []Reference{{Oid: oid1, Name: "HEAD"}, {Oid: oid2, Name: "refs/heads/master"}}, d.Refs)
}

func TestInvalidHeaderFails(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "# service=invalid\n")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, oid1+" HEAD\x00caps")
	gittest.WritePktlineFlush(t, buf)

	_, err := ParseReferenceDiscovery(buf)
	require.Error(t, err)
}

func TestMissingRefsFail(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "# service=git-upload-pack\n")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineFlush(t, buf)

	_, err := ParseReferenceDiscovery(buf)
	require.Error(t, err)
}

func TestInvalidRefFail(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "# service=git-upload-pack\n")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, oid1+" HEAD\x00caps")
	gittest.WritePktlineString(t, buf, oid2)
	gittest.WritePktlineFlush(t, buf)

	_, err := ParseReferenceDiscovery(buf)
	require.Error(t, err)
}

func TestMissingTrailingFlushFails(t *testing.T) {
	buf := &bytes.Buffer{}
	gittest.WritePktlineString(t, buf, "# service=git-upload-pack\n")
	gittest.WritePktlineFlush(t, buf)
	gittest.WritePktlineString(t, buf, oid1+" HEAD\x00caps")

	d := ReferenceDiscovery{}
	require.Error(t, d.Parse(buf))
}
