package catfile

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

func setupBatch(t *testing.T, ctx context.Context) (Batch, testhelper.Cleanup) {
	t.Helper()

	var deferrer testhelper.Deferrer
	defer deferrer.Call()

	cfgBuilder := testcfg.NewGitalyCfgBuilder()
	deferrer.Add(cfgBuilder.Cleanup)
	cfg, repos := cfgBuilder.BuildWithRepoAt(t, t.Name())

	c, err := New(ctx, git.NewExecCommandFactory(cfg), repos[0])
	require.NoError(t, err)

	cleaner := deferrer.Relocate()
	return c, cleaner.Call
}

func TestInfo(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, cleanup := setupBatch(t, ctx)
	defer cleanup()

	testCases := []struct {
		desc     string
		revision string
		output   *ObjectInfo
	}{
		{
			desc:     "gitignore",
			revision: "60ecb67744cb56576c30214ff52294f8ce2def98:.gitignore",
			output: &ObjectInfo{
				Oid:  "dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82",
				Type: "blob",
				Size: 241,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			oi, err := c.Info(ctx, git.Revision(tc.revision))
			require.NoError(t, err)

			require.Equal(t, tc.output, oi)
		})
	}
}

func TestBlob(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, cleanup := setupBatch(t, ctx)
	defer cleanup()

	gitignoreBytes, err := ioutil.ReadFile("testdata/blob-dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82")
	require.NoError(t, err)

	testCases := []struct {
		desc       string
		revision   string
		objInfo    ObjectInfo
		content    []byte
		requireErr func(*testing.T, error)
	}{
		{
			desc:     "gitignore",
			revision: "60ecb67744cb56576c30214ff52294f8ce2def98:.gitignore",
			objInfo: ObjectInfo{
				Oid:  "dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82",
				Type: "blob",
				Size: int64(len(gitignoreBytes)),
			},
			content: gitignoreBytes,
		},
		{
			desc:     "not existing ref",
			revision: "stub",
			requireErr: func(t *testing.T, err error) {
				require.True(t, IsNotFound(err), "the error must be from 'not found' family")
				require.EqualError(t, err, "object not found")
			},
		},
		{
			desc:     "wrong object type",
			revision: "1e292f8fedd741b75372e19097c76d327140c312", // is commit SHA1
			requireErr: func(t *testing.T, err error) {
				require.True(t, IsNotFound(err), "the error must be from 'not found' family")
				require.EqualError(t, err, "expected 1e292f8fedd741b75372e19097c76d327140c312 to be a blob, got commit")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			blobObj, err := c.Blob(ctx, git.Revision(tc.revision))

			if tc.requireErr != nil {
				tc.requireErr(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.objInfo, blobObj.ObjectInfo)

			contents, err := ioutil.ReadAll(blobObj.Reader)
			require.NoError(t, err)
			require.Equal(t, tc.content, contents)
		})
	}
}

func TestCommit(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, cleanup := setupBatch(t, ctx)
	defer cleanup()

	commitBytes, err := ioutil.ReadFile("testdata/commit-e63f41fe459e62e1228fcef60d7189127aeba95a")
	require.NoError(t, err)

	testCases := []struct {
		desc     string
		revision string
		output   string
	}{
		{
			desc:     "commit with non-oid spec",
			revision: "60ecb67744cb56576c30214ff52294f8ce2def98^",
			output:   string(commitBytes),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			commitReader, err := c.Commit(ctx, git.Revision(tc.revision))
			require.NoError(t, err)

			contents, err := ioutil.ReadAll(commitReader)
			require.NoError(t, err)

			require.Equal(t, tc.output, string(contents))
		})
	}
}

func TestTag(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, cleanup := setupBatch(t, ctx)
	defer cleanup()

	tagBytes, err := ioutil.ReadFile("testdata/tag-a509fa67c27202a2bc9dd5e014b4af7e6063ac76")
	require.NoError(t, err)

	testCases := []struct {
		desc       string
		revision   string
		objInfo    ObjectInfo
		content    []byte
		requireErr func(*testing.T, error)
	}{
		{
			desc:     "tag",
			revision: "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8",
			objInfo: ObjectInfo{
				Oid:  "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8",
				Type: "tag",
				Size: int64(len(tagBytes)),
			},
			content: tagBytes,
		},
		{
			desc:     "not existing ref",
			revision: "stub",
			requireErr: func(t *testing.T, err error) {
				require.True(t, IsNotFound(err), "the error must be from 'not found' family")
				require.EqualError(t, err, "object not found")
			},
		},
		{
			desc:     "wrong object type",
			revision: "1e292f8fedd741b75372e19097c76d327140c312", // is commit SHA1
			requireErr: func(t *testing.T, err error) {
				require.True(t, IsNotFound(err), "the error must be from 'not found' family")
				require.EqualError(t, err, "expected 1e292f8fedd741b75372e19097c76d327140c312 to be a tag, got commit")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tagObj, err := c.Tag(ctx, git.Revision(tc.revision))

			if tc.requireErr != nil {
				tc.requireErr(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.objInfo, tagObj.ObjectInfo)

			contents, err := ioutil.ReadAll(tagObj.Reader)
			require.NoError(t, err)
			require.Equal(t, tc.content, contents)
		})
	}
}

func TestTree(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, cleanup := setupBatch(t, ctx)
	defer cleanup()

	treeBytes, err := ioutil.ReadFile("testdata/tree-7e2f26d033ee47cd0745649d1a28277c56197921")
	require.NoError(t, err)

	testCases := []struct {
		desc       string
		revision   string
		objInfo    ObjectInfo
		content    []byte
		requireErr func(*testing.T, error)
	}{
		{
			desc:     "tree with non-oid spec",
			revision: "60ecb67744cb56576c30214ff52294f8ce2def98^{tree}",
			objInfo: ObjectInfo{
				Oid:  "7e2f26d033ee47cd0745649d1a28277c56197921",
				Type: "tree",
				Size: int64(len(treeBytes)),
			},
			content: treeBytes,
		},
		{
			desc:     "not existing ref",
			revision: "stud",
			requireErr: func(t *testing.T, err error) {
				require.True(t, IsNotFound(err), "the error must be from 'not found' family")
				require.EqualError(t, err, "object not found")
			},
		},
		{
			desc:     "wrong object type",
			revision: "1e292f8fedd741b75372e19097c76d327140c312", // is commit SHA1
			requireErr: func(t *testing.T, err error) {
				require.True(t, IsNotFound(err), "the error must be from 'not found' family")
				require.EqualError(t, err, "expected 1e292f8fedd741b75372e19097c76d327140c312 to be a tree, got commit")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			treeObj, err := c.Tree(ctx, git.Revision(tc.revision))

			if tc.requireErr != nil {
				tc.requireErr(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.objInfo, treeObj.ObjectInfo)

			contents, err := ioutil.ReadAll(treeObj.Reader)
			require.NoError(t, err)
			require.Equal(t, tc.content, contents)
		})
	}
}

func TestRepeatedCalls(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, cleanup := setupBatch(t, ctx)
	defer cleanup()

	treeOid := git.Revision("7e2f26d033ee47cd0745649d1a28277c56197921")
	treeBytes, err := ioutil.ReadFile("testdata/tree-7e2f26d033ee47cd0745649d1a28277c56197921")
	require.NoError(t, err)

	tree1Obj, err := c.Tree(ctx, treeOid)
	require.NoError(t, err)

	tree1, err := ioutil.ReadAll(tree1Obj.Reader)
	require.NoError(t, err)

	require.Equal(t, string(treeBytes), string(tree1))

	blobReader, err := c.Blob(ctx, "dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82")
	require.NoError(t, err)

	_, err = c.Tree(ctx, treeOid)
	require.Error(t, err, "request should fail because of unconsumed blob data")

	_, err = io.CopyN(ioutil.Discard, blobReader, 10)
	require.NoError(t, err)

	_, err = c.Tree(ctx, treeOid)
	require.Error(t, err, "request should fail because of unconsumed blob data")

	_, err = io.Copy(ioutil.Discard, blobReader)
	require.NoError(t, err, "blob reading should still work")

	tree2Obj, err := c.Tree(ctx, treeOid)
	require.NoError(t, err)

	tree2, err := ioutil.ReadAll(tree2Obj.Reader)
	require.NoError(t, err, "request should succeed because blob was consumed")

	require.Equal(t, string(treeBytes), string(tree2))
}

func TestSpawnFailure(t *testing.T) {
	defer func() { injectSpawnErrors = false }()

	// reset global cache
	defer func(old *batchCache) { cache = old }(cache)

	// Use very high values to effectively disable auto-expiry
	cache = newCache(1*time.Hour, 1000)
	defer cache.EvictAll()

	require.True(
		t,
		waitTrue(func() bool { return numGitChildren(t) == 0 }),
		"test setup: wait for there to be 0 git children",
	)
	require.Equal(t, 0, cacheSize(cache), "sanity check: cache empty")

	ctx1, cancel1 := testhelper.Context()
	defer cancel1()

	cfgBuilder := testcfg.NewGitalyCfgBuilder(testcfg.WithStorages("storage"))
	defer cfgBuilder.Cleanup()
	cfg, repos := cfgBuilder.BuildWithRepoAt(t, t.Name())
	testRepo := repos[0]

	gitCmdFactory := git.NewExecCommandFactory(cfg)

	injectSpawnErrors = false
	_, err := catfileWithFreshSessionID(ctx1, gitCmdFactory, testRepo)
	require.NoError(t, err, "catfile spawn should succeed in normal circumstances")
	require.Equal(t, 2, numGitChildren(t), "there should be 2 git child processes")

	// cancel request context: this should asynchronously move the processes into the cat-file cache
	cancel1()

	require.True(
		t,
		waitTrue(func() bool { return cacheSize(cache) == 1 }),
		"1 cache entry, meaning 2 processes, should be in the cache now",
	)

	require.Equal(t, 2, numGitChildren(t), "there should still be 2 git child processes")

	cache.EvictAll()
	require.Equal(t, 0, cacheSize(cache), "the cache should be empty now")

	require.True(
		t,
		waitTrue(func() bool { return numGitChildren(t) == 0 }),
		"number of git processes should drop to 0 again",
	)

	ctx2, cancel2 := testhelper.Context()
	defer cancel2()

	injectSpawnErrors = true
	_, err = catfileWithFreshSessionID(ctx2, gitCmdFactory, testRepo)
	require.Error(t, err, "expect simulated error")
	require.IsType(t, &simulatedBatchSpawnError{}, err)

	require.True(
		t,
		waitTrue(func() bool { return numGitChildren(t) == 0 }),
		"there should be no git children after spawn failure scenario",
	)
}

func catfileWithFreshSessionID(ctx context.Context, gitCmdFactory git.CommandFactory, repo *gitalypb.Repository) (Batch, error) {
	id, err := text.RandomHex(4)
	if err != nil {
		return nil, err
	}

	md := metadata.New(map[string]string{
		SessionIDField: id,
	})

	return New(metadata.NewIncomingContext(ctx, md), gitCmdFactory, repo)
}

func waitTrue(callback func() bool) bool {
	for start := time.Now(); time.Since(start) < 1*time.Second; time.Sleep(1 * time.Millisecond) {
		if callback() {
			return true
		}
	}

	return false
}

func numGitChildren(t *testing.T) int {
	out, err := exec.Command("pgrep", "-x", "-P", strconv.Itoa(os.Getpid()), "git").Output()

	if err != nil {
		if code, ok := command.ExitStatus(err); ok && code == 1 {
			// pgrep exit code 1 means: no processes found
			return 0
		}

		t.Fatal(err)
	}

	return bytes.Count(out, []byte("\n"))
}

func cacheSize(bc *batchCache) int {
	bc.Lock()
	defer bc.Unlock()
	return bc.len()
}
