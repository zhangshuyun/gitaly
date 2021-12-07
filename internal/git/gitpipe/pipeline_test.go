package gitpipe

import (
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestPipeline_revlist(t *testing.T) {
	cfg := testcfg.Build(t)

	repoProto, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	for _, tc := range []struct {
		desc               string
		revisions          []string
		revlistOptions     []RevlistOption
		catfileInfoOptions []CatfileInfoOption
		expectedResults    []CatfileObjectResult
		expectedErr        error
	}{
		{
			desc: "single blob",
			revisions: []string{
				lfsPointer1,
			},
			revlistOptions: []RevlistOption{
				WithObjects(),
			},
			expectedResults: []CatfileObjectResult{
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}}},
			},
		},
		{
			desc: "single blob without objects",
			revisions: []string{
				lfsPointer1,
			},
			expectedResults: nil,
		},
		{
			desc: "multiple blobs",
			revisions: []string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
			},
			revlistOptions: []RevlistOption{
				WithObjects(),
			},
			expectedResults: []CatfileObjectResult{
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}}},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}}},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: lfsPointer3, Type: "blob", Size: 127}}},
			},
		},
		{
			desc: "multiple blobs with filter",
			revisions: []string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
			},
			revlistOptions: []RevlistOption{
				WithObjects(),
				WithSkipRevlistResult(func(r *RevisionResult) bool {
					return r.OID != lfsPointer2
				}),
			},
			expectedResults: []CatfileObjectResult{
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}}},
			},
		},
		{
			desc: "tree",
			revisions: []string{
				"b95c0fad32f4361845f91d9ce4c1721b52b82793",
			},
			revlistOptions: []RevlistOption{
				WithObjects(),
			},
			expectedResults: []CatfileObjectResult{
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793", Type: "tree", Size: 43}}},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}}, ObjectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "tree without objects",
			revisions: []string{
				"b95c0fad32f4361845f91d9ce4c1721b52b82793",
			},
			expectedResults: nil,
		},
		{
			desc: "tree with blob filter",
			revisions: []string{
				"b95c0fad32f4361845f91d9ce4c1721b52b82793",
			},
			revlistOptions: []RevlistOption{
				WithObjects(),
			},
			catfileInfoOptions: []CatfileInfoOption{
				WithSkipCatfileInfoResult(func(objectInfo *catfile.ObjectInfo) bool {
					return objectInfo.Type != "blob"
				}),
			},
			expectedResults: []CatfileObjectResult{
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}}, ObjectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "revision range",
			revisions: []string{
				"^master~",
				"master",
			},
			revlistOptions: []RevlistOption{
				WithObjects(),
			},
			expectedResults: []CatfileObjectResult{
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "1e292f8fedd741b75372e19097c76d327140c312", Type: "commit", Size: 388}}},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "07f8147e8e73aab6c935c296e8cdc5194dee729b", Type: "tree", Size: 780}}},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "ceb102b8d3f9a95c2eb979213e49f7cc1b23d56e", Type: "tree", Size: 258}}, ObjectName: []byte("files")},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "2132d150328bd9334cc4e62a16a5d998a7e399b9", Type: "tree", Size: 31}}, ObjectName: []byte("files/flat")},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "f3942dc8b824a2c9359e518d48e68f84461bd2f7", Type: "tree", Size: 34}}, ObjectName: []byte("files/flat/path")},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "ea7249055466085d0a6c69951908ef47757e92f4", Type: "tree", Size: 39}}, ObjectName: []byte("files/flat/path/correct")},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "c1c67abbaf91f624347bb3ae96eabe3a1b742478", Type: "commit", Size: 326}}},
			},
		},
		{
			desc: "revision range without objects",
			revisions: []string{
				"^master~",
				"master",
			},
			expectedResults: []CatfileObjectResult{
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "1e292f8fedd741b75372e19097c76d327140c312", Type: "commit", Size: 388}}},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: "c1c67abbaf91f624347bb3ae96eabe3a1b742478", Type: "commit", Size: 326}}},
			},
		},
		{
			desc: "--all with all filters",
			revisions: []string{
				"--all",
			},
			revlistOptions: []RevlistOption{
				WithObjects(),
				WithSkipRevlistResult(func(r *RevisionResult) bool {
					// Let through two LFS pointers and a tree.
					return r.OID != "b95c0fad32f4361845f91d9ce4c1721b52b82793" &&
						r.OID != lfsPointer1 && r.OID != lfsPointer2
				}),
			},
			catfileInfoOptions: []CatfileInfoOption{
				WithSkipCatfileInfoResult(func(objectInfo *catfile.ObjectInfo) bool {
					// Only let through blobs, so only the two LFS pointers remain.
					return objectInfo.Type != "blob"
				}),
			},
			expectedResults: []CatfileObjectResult{
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}}, ObjectName: []byte("files/lfs/lfs_object.iso")},
				{Object: &catfile.Object{ObjectInfo: catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}}, ObjectName: []byte("another.lfs")},
			},
		},
		{
			desc: "invalid revision",
			revisions: []string{
				"doesnotexist",
			},
			expectedErr: errors.New("rev-list pipeline command: exit status 128, stderr: " +
				"\"fatal: ambiguous argument 'doesnotexist': unknown revision or path not in the working tree.\\n" +
				"Use '--' to separate paths from revisions, like this:\\n" +
				"'git <command> [<revision>...] -- [<file>...]'\\n\""),
		},
		{
			desc: "mixed valid and invalid revision",
			revisions: []string{
				lfsPointer1,
				"doesnotexist",
				lfsPointer2,
			},
			expectedErr: errors.New("rev-list pipeline command: exit status 128, stderr: " +
				"\"fatal: ambiguous argument 'doesnotexist': unknown revision or path not in the working tree.\\n" +
				"Use '--' to separate paths from revisions, like this:\\n" +
				"'git <command> [<revision>...] -- [<file>...]'\\n\""),
		},
		{
			desc: "invalid revision with all filters",
			revisions: []string{
				"doesnotexist",
			},
			revlistOptions: []RevlistOption{
				WithSkipRevlistResult(func(r *RevisionResult) bool {
					require.Fail(t, "filter should not be invoked on errors")
					return true
				}),
			},
			catfileInfoOptions: []CatfileInfoOption{
				WithSkipCatfileInfoResult(func(r *catfile.ObjectInfo) bool {
					require.Fail(t, "filter should not be invoked on errors")
					return true
				}),
			},
			expectedErr: errors.New("rev-list pipeline command: exit status 128, stderr: " +
				"\"fatal: ambiguous argument 'doesnotexist': unknown revision or path not in the working tree.\\n" +
				"Use '--' to separate paths from revisions, like this:\\n" +
				"'git <command> [<revision>...] -- [<file>...]'\\n\""),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			catfileCache := catfile.NewCache(cfg)
			defer catfileCache.Stop()

			objectInfoReader, err := catfileCache.ObjectInfoReader(ctx, repo)
			require.NoError(t, err)

			objectReader, err := catfileCache.ObjectReader(ctx, repo)
			require.NoError(t, err)

			revlistIter := Revlist(ctx, repo, tc.revisions, tc.revlistOptions...)

			catfileInfoIter, err := CatfileInfo(ctx, objectInfoReader, revlistIter, tc.catfileInfoOptions...)
			require.NoError(t, err)

			catfileObjectIter, err := CatfileObject(ctx, objectReader, catfileInfoIter)
			require.NoError(t, err)

			var results []CatfileObjectResult
			for catfileObjectIter.Next() {
				result := catfileObjectIter.Result()

				objectData, err := io.ReadAll(result)
				require.NoError(t, err)
				require.Len(t, objectData, int(result.ObjectSize()))

				// We only really want to compare the publicly visible fields
				// containing info about the object itself, and not the object's
				// private state. We thus need to reconstruct the objects here.
				results = append(results, CatfileObjectResult{
					Object: &catfile.Object{
						ObjectInfo: catfile.ObjectInfo{
							Oid:  result.ObjectID(),
							Type: result.ObjectType(),
							Size: result.ObjectSize(),
						},
					},
					ObjectName: result.ObjectName,
				})
			}

			// We're converting the error here to a plain un-nested error such that we
			// don't have to replicate the complete error's structure.
			err = catfileObjectIter.Err()
			if err != nil {
				err = errors.New(err.Error())
			}

			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedResults, results)
		})
	}

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		catfileCache := catfile.NewCache(cfg)
		defer catfileCache.Stop()

		objectInfoReader, err := catfileCache.ObjectInfoReader(ctx, repo)
		require.NoError(t, err)

		objectReader, err := catfileCache.ObjectReader(ctx, repo)
		require.NoError(t, err)

		revlistIter := Revlist(ctx, repo, []string{"--all"})

		catfileInfoIter, err := CatfileInfo(ctx, objectInfoReader, revlistIter)
		require.NoError(t, err)

		catfileObjectIter, err := CatfileObject(ctx, objectReader, catfileInfoIter)
		require.NoError(t, err)

		i := 0
		for catfileObjectIter.Next() {
			i++

			_, err := io.Copy(io.Discard, catfileObjectIter.Result())
			require.NoError(t, err)

			if i == 3 {
				cancel()
			}
		}

		require.NoError(t, catfileObjectIter.Err())

		// Context cancellation is timing sensitive: at the point of cancelling the context,
		// the last pipeline step may already have queued up an additional result. We thus
		// cannot assert the exact number of requests, but we know that it's bounded.
		require.LessOrEqual(t, i, 4)
	})

	t.Run("interleaving object reads", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		catfileCache := catfile.NewCache(cfg)
		defer catfileCache.Stop()

		objectInfoReader, err := catfileCache.ObjectInfoReader(ctx, repo)
		require.NoError(t, err)

		objectReader, err := catfileCache.ObjectReader(ctx, repo)
		require.NoError(t, err)

		revlistIter := Revlist(ctx, repo, []string{"--all"}, WithObjects())

		catfileInfoIter, err := CatfileInfo(ctx, objectInfoReader, revlistIter)
		require.NoError(t, err)

		catfileObjectIter, err := CatfileObject(ctx, objectReader, catfileInfoIter)
		require.NoError(t, err)

		i := 0
		var wg sync.WaitGroup
		for catfileObjectIter.Next() {
			wg.Add(1)
			i++

			// With the catfile package, one mustn't ever request a new object before
			// the old object's reader was completely consumed. We cannot reliably test
			// this given that the object channel, if it behaves correctly, will block
			// until we've read the old object. Chances are high though that we'd
			// eventually hit the race here in case we didn't correctly synchronize on
			// the object reader.
			go func(object CatfileObjectResult) {
				defer wg.Done()
				_, err := io.Copy(io.Discard, object)
				require.NoError(t, err)
			}(catfileObjectIter.Result())
		}

		require.NoError(t, catfileObjectIter.Err())
		wg.Wait()

		// We could in theory assert the exact amount of objects, but this would make it
		// harder than necessary to change the test repo's contents.
		require.Greater(t, i, 1000)
	})
}

func TestPipeline_forEachRef(t *testing.T) {
	cfg := testcfg.Build(t)

	repoProto, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0])
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	ctx, cancel := testhelper.Context()
	defer cancel()

	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()

	objectInfoReader, err := catfileCache.ObjectInfoReader(ctx, repo)
	require.NoError(t, err)

	objectReader, err := catfileCache.ObjectReader(ctx, repo)
	require.NoError(t, err)

	forEachRefIter := ForEachRef(ctx, repo, nil)

	catfileInfoIter, err := CatfileInfo(ctx, objectInfoReader, forEachRefIter)
	require.NoError(t, err)

	catfileObjectIter, err := CatfileObject(ctx, objectReader, catfileInfoIter)
	require.NoError(t, err)

	type object struct {
		oid     git.ObjectID
		content []byte
	}

	objectsByRef := make(map[git.ReferenceName]object)
	for catfileObjectIter.Next() {
		result := catfileObjectIter.Result()

		objectData, err := io.ReadAll(result)
		require.NoError(t, err)
		require.Len(t, objectData, int(result.ObjectSize()))

		objectsByRef[git.ReferenceName(result.ObjectName)] = object{
			oid:     result.ObjectID(),
			content: objectData,
		}
	}
	require.NoError(t, catfileObjectIter.Err())
	require.Greater(t, len(objectsByRef), 90)

	// We certainly don't want to hard-code all the references, so we just cross-check with the
	// localrepo implementation to verify that both return the same data.
	refs, err := repo.GetReferences(ctx)
	require.NoError(t, err)
	require.Equal(t, len(refs), len(objectsByRef))

	expectedObjectsByRef := make(map[git.ReferenceName]object)
	for _, ref := range refs {
		oid := git.ObjectID(ref.Target)
		content, err := repo.ReadObject(ctx, oid)
		require.NoError(t, err)

		expectedObjectsByRef[ref.Name] = object{
			oid:     oid,
			content: content,
		}
	}
	require.Equal(t, expectedObjectsByRef, objectsByRef)
}
