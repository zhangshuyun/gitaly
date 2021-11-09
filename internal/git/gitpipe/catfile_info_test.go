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

	repoProto, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	for _, tc := range []struct {
		desc            string
		revlistInputs   []RevisionResult
		opts            []CatfileInfoOption
		expectedResults []CatfileInfoResult
		expectedErr     error
	}{
		{
			desc: "single blob",
			revlistInputs: []RevisionResult{
				{OID: lfsPointer1},
			},
			expectedResults: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
			},
		},
		{
			desc: "multiple blobs",
			revlistInputs: []RevisionResult{
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
			revlistInputs: []RevisionResult{
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
			revlistInputs: []RevisionResult{
				{OID: "invalidobjectid"},
			},
			expectedErr: errors.New("retrieving object info for \"invalidobjectid\": object not found"),
		},
		{
			desc: "mixed valid and invalid revision",
			revlistInputs: []RevisionResult{
				{OID: lfsPointer1},
				{OID: "invalidobjectid"},
				{OID: lfsPointer2},
			},
			expectedResults: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
			},
			expectedErr: errors.New("retrieving object info for \"invalidobjectid\": object not found"),
		},
		{
			desc: "skip everything",
			revlistInputs: []RevisionResult{
				{OID: lfsPointer1},
				{OID: lfsPointer2},
			},
			opts: []CatfileInfoOption{
				WithSkipCatfileInfoResult(func(*catfile.ObjectInfo) bool { return true }),
			},
		},
		{
			desc: "skip one",
			revlistInputs: []RevisionResult{
				{OID: lfsPointer1},
				{OID: lfsPointer2},
			},
			opts: []CatfileInfoOption{
				WithSkipCatfileInfoResult(func(objectInfo *catfile.ObjectInfo) bool {
					return objectInfo.Oid == lfsPointer1
				}),
			},
			expectedResults: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
			},
		},
		{
			desc: "skip nothing",
			revlistInputs: []RevisionResult{
				{OID: lfsPointer1},
				{OID: lfsPointer2},
			},
			opts: []CatfileInfoOption{
				WithSkipCatfileInfoResult(func(*catfile.ObjectInfo) bool { return false }),
			},
			expectedResults: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			catfileCache := catfile.NewCache(cfg)
			defer catfileCache.Stop()

			objectInfoReader, err := catfileCache.ObjectInfoReader(ctx, repo)
			require.NoError(t, err)

			it, err := CatfileInfo(ctx, objectInfoReader, NewRevisionIterator(tc.revlistInputs), tc.opts...)
			require.NoError(t, err)

			var results []CatfileInfoResult
			for it.Next() {
				results = append(results, it.Result())
			}

			// We're converting the error here to a plain un-nested error such
			// that we don't have to replicate the complete error's structure.
			err = it.Err()
			if err != nil {
				err = errors.New(err.Error())
			}

			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedResults, results)
		})
	}
}

func TestCatfileInfoAllObjects(t *testing.T) {
	cfg := testcfg.Build(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	repoProto, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	blob1 := gittest.WriteBlob(t, cfg, repoPath, []byte("foobar"))
	blob2 := gittest.WriteBlob(t, cfg, repoPath, []byte("barfoo"))
	tree := gittest.WriteTree(t, cfg, repoPath, []gittest.TreeEntry{
		{Path: "foobar", Mode: "100644", OID: blob1},
	})
	commit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithParents())

	it := CatfileInfoAllObjects(ctx, repo)

	var results []CatfileInfoResult
	for it.Next() {
		results = append(results, it.Result())
	}
	require.NoError(t, it.Err())

	require.ElementsMatch(t, []CatfileInfoResult{
		{ObjectInfo: &catfile.ObjectInfo{Oid: blob1, Type: "blob", Size: 6}},
		{ObjectInfo: &catfile.ObjectInfo{Oid: blob2, Type: "blob", Size: 6}},
		{ObjectInfo: &catfile.ObjectInfo{Oid: tree, Type: "tree", Size: 34}},
		{ObjectInfo: &catfile.ObjectInfo{Oid: commit, Type: "commit", Size: 177}},
	}, results)
}
