package gitpipe

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

const (
	lfsPointer1 = "0c304a93cb8430108629bbbcaa27db3343299bc0"
	lfsPointer2 = "f78df813119a79bfbe0442ab92540a61d3ab7ff3"
	lfsPointer3 = "bab31d249f78fba464d1b75799aad496cc07fa3b"
	lfsPointer4 = "125fcc9f6e33175cb278b9b2809154d2535fe19f"
)

func TestCatfileInfo(t *testing.T) {
	cfg := testcfg.Build(t)

	repoProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	defer cleanup()
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	for _, tc := range []struct {
		desc            string
		revlistInputs   []RevlistResult
		expectedResults []CatfileInfoResult
	}{
		{
			desc: "single blob",
			revlistInputs: []RevlistResult{
				{OID: lfsPointer1},
			},
			expectedResults: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
			},
		},
		{
			desc: "multiple blobs",
			revlistInputs: []RevlistResult{
				{OID: lfsPointer1},
				{OID: lfsPointer2},
				{OID: lfsPointer3},
				{OID: lfsPointer4},
			},
			expectedResults: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer3, Type: "blob", Size: 127}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer4, Type: "blob", Size: 129}},
			},
		},
		{
			desc: "object name",
			revlistInputs: []RevlistResult{
				{OID: "b95c0fad32f4361845f91d9ce4c1721b52b82793"},
				{OID: "93e123ac8a3e6a0b600953d7598af629dec7b735", ObjectName: []byte("branch-test.txt")},
			},
			expectedResults: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793", Type: "tree", Size: 43}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}, ObjectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "invalid object ID",
			revlistInputs: []RevlistResult{
				{OID: "invalidobjectid"},
			},
			expectedResults: []CatfileInfoResult{
				{Err: errors.New("retrieving object info for \"invalidobjectid\": object not found")},
			},
		},
		{
			desc: "mixed valid and invalid revision",
			revlistInputs: []RevlistResult{
				{OID: lfsPointer1},
				{OID: "invalidobjectid"},
				{OID: lfsPointer2},
			},
			expectedResults: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{Err: errors.New("retrieving object info for \"invalidobjectid\": object not found")},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			catfileCache := catfile.NewCache(cfg)
			defer catfileCache.Stop()

			catfileProcess, err := catfileCache.BatchProcess(ctx, repo)
			require.NoError(t, err)

			resultChan := CatfileInfo(ctx, catfileProcess, NewRevlistIterator(tc.revlistInputs))

			var results []CatfileInfoResult
			for result := range resultChan {
				// We're converting the error here to a plain un-nested error such
				// that we don't have to replicate the complete error's structure.
				if result.Err != nil {
					result.Err = errors.New(result.Err.Error())
				}

				results = append(results, result)
			}

			require.Equal(t, tc.expectedResults, results)
		})
	}
}

func TestCatfileInfoAllObjects(t *testing.T) {
	cfg := testcfg.Build(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	repoProto, repoPath, cleanup := gittest.InitBareRepoAt(t, cfg, cfg.Storages[0])
	defer cleanup()
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foobar"))
	blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("barfoo"))
	tree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "foobar", Mode: "100644", OID: blob1},
	})
	commit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())

	resultChan := CatfileInfoAllObjects(ctx, repo)

	var results []CatfileInfoResult
	for result := range resultChan {
		require.NoError(t, result.Err)
		results = append(results, result)
	}

	require.ElementsMatch(t, []CatfileInfoResult{
		{ObjectInfo: &catfile.ObjectInfo{Oid: blob1, Type: "blob", Size: 6}},
		{ObjectInfo: &catfile.ObjectInfo{Oid: blob2, Type: "blob", Size: 6}},
		{ObjectInfo: &catfile.ObjectInfo{Oid: tree, Type: "tree", Size: 34}},
		{ObjectInfo: &catfile.ObjectInfo{Oid: commit, Type: "commit", Size: 177}},
	}, results)
}

func TestCatfileInfoFilter(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		input           []CatfileInfoResult
		filter          func(CatfileInfoResult) bool
		expectedResults []CatfileInfoResult
	}{
		{
			desc: "all accepted",
			input: []CatfileInfoResult{
				{ObjectName: []byte{'a'}},
				{ObjectName: []byte{'b'}},
				{ObjectName: []byte{'c'}},
			},
			filter: func(CatfileInfoResult) bool {
				return true
			},
			expectedResults: []CatfileInfoResult{
				{ObjectName: []byte{'a'}},
				{ObjectName: []byte{'b'}},
				{ObjectName: []byte{'c'}},
			},
		},
		{
			desc: "all filtered",
			input: []CatfileInfoResult{
				{ObjectName: []byte{'a'}},
				{ObjectName: []byte{'b'}},
				{ObjectName: []byte{'c'}},
			},
			filter: func(CatfileInfoResult) bool {
				return false
			},
		},
		{
			desc: "errors always get through",
			input: []CatfileInfoResult{
				{ObjectName: []byte{'a'}},
				{ObjectName: []byte{'b'}},
				{Err: errors.New("foobar")},
				{ObjectName: []byte{'c'}},
			},
			filter: func(CatfileInfoResult) bool {
				return false
			},
			expectedResults: []CatfileInfoResult{
				{Err: errors.New("foobar")},
			},
		},
		{
			desc: "subset filtered",
			input: []CatfileInfoResult{
				{ObjectName: []byte{'a'}},
				{ObjectName: []byte{'b'}},
				{ObjectName: []byte{'c'}},
			},
			filter: func(r CatfileInfoResult) bool {
				return r.ObjectName[0] == 'b'
			},
			expectedResults: []CatfileInfoResult{
				{ObjectName: []byte{'b'}},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			inputChan := make(chan CatfileInfoResult, len(tc.input))
			for _, input := range tc.input {
				inputChan <- input
			}
			close(inputChan)

			var results []CatfileInfoResult
			for result := range CatfileInfoFilter(ctx, inputChan, tc.filter) {
				results = append(results, result)
			}

			require.Equal(t, tc.expectedResults, results)
		})
	}
}
