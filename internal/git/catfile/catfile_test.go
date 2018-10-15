package catfile

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestInfo(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := New(ctx, testhelper.TestRepository())
	require.NoError(t, err)

	testCases := []struct {
		desc   string
		spec   string
		output *ObjectInfo
	}{
		{
			desc: "gitignore",
			spec: "60ecb67744cb56576c30214ff52294f8ce2def98:.gitignore",
			output: &ObjectInfo{
				Oid:  "dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82",
				Type: "blob",
				Size: 241,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			oi, err := c.Info(tc.spec)
			require.NoError(t, err)

			require.Equal(t, tc.output, oi)
		})
	}
}

func TestBlob(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := New(ctx, testhelper.TestRepository())
	require.NoError(t, err)

	gitignoreBytes, err := ioutil.ReadFile("testdata/blob-dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82")
	require.NoError(t, err)

	testCases := []struct {
		desc   string
		spec   string
		output string
	}{
		{
			desc:   "gitignore",
			spec:   "60ecb67744cb56576c30214ff52294f8ce2def98:.gitignore",
			output: string(gitignoreBytes),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			r, err := c.Blob(tc.spec)
			require.NoError(t, err)

			contents, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, tc.output, string(contents))
		})
	}
}

func TestCommit(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := New(ctx, testhelper.TestRepository())
	require.NoError(t, err)

	commitBytes, err := ioutil.ReadFile("testdata/commit-e63f41fe459e62e1228fcef60d7189127aeba95a")
	require.NoError(t, err)

	testCases := []struct {
		desc   string
		spec   string
		output string
	}{
		{
			desc:   "commit with non-oid spec",
			spec:   "60ecb67744cb56576c30214ff52294f8ce2def98^",
			output: string(commitBytes),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			commitReader, err := c.Commit(tc.spec)
			require.NoError(t, err)

			contents, err := ioutil.ReadAll(commitReader)
			require.NoError(t, err)

			require.Equal(t, tc.output, string(contents))
		})
	}

}

func TestTree(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := New(ctx, testhelper.TestRepository())
	require.NoError(t, err)

	treeBytes, err := ioutil.ReadFile("testdata/tree-7e2f26d033ee47cd0745649d1a28277c56197921")
	require.NoError(t, err)

	testCases := []struct {
		desc   string
		spec   string
		output string
	}{
		{
			desc:   "tree with non-oid spec",
			spec:   "60ecb67744cb56576c30214ff52294f8ce2def98^{tree}",
			output: string(treeBytes),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			treeReader, err := c.Tree(tc.spec)
			require.NoError(t, err)

			contents, err := ioutil.ReadAll(treeReader)
			require.NoError(t, err)

			require.Equal(t, tc.output, string(contents))
		})
	}

}

func TestRepeatedCalls(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := New(ctx, testhelper.TestRepository())
	require.NoError(t, err)

	treeOid := "7e2f26d033ee47cd0745649d1a28277c56197921"
	treeBytes, err := ioutil.ReadFile("testdata/tree-7e2f26d033ee47cd0745649d1a28277c56197921")
	require.NoError(t, err)

	tree1Reader, err := c.Tree(treeOid)
	require.NoError(t, err)

	tree1, err := ioutil.ReadAll(tree1Reader)
	require.NoError(t, err)

	require.Equal(t, string(treeBytes), string(tree1))

	blobReader, err := c.Blob("dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82")
	require.NoError(t, err)

	_, err = c.Tree(treeOid)
	require.Error(t, err, "request should fail because of unconsumed blob data")

	_, err = io.CopyN(ioutil.Discard, blobReader, 10)
	require.NoError(t, err)

	_, err = c.Tree(treeOid)
	require.Error(t, err, "request should fail because of unconsumed blob data")

	_, err = io.Copy(ioutil.Discard, blobReader)
	require.NoError(t, err, "blob reading should still work")

	tree2Reader, err := c.Tree(treeOid)
	require.NoError(t, err)

	tree2, err := ioutil.ReadAll(tree2Reader)
	require.NoError(t, err, "request should succeed because blob was consumed")

	require.Equal(t, string(treeBytes), string(tree2))
}
