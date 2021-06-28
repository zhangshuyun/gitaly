package blob

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
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

func TestRevlist(t *testing.T) {
	cfg := testcfg.Build(t)

	repoProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	defer cleanup()
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	needsObjectTypeFilters := func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		gitVersion, err := git.CurrentVersion(ctx, git.NewExecCommandFactory(cfg))
		require.NoError(t, err)

		if !gitVersion.SupportsObjectTypeFilter() {
			t.Skip("Git does not support object type filters")
		}
	}

	for _, tc := range []struct {
		desc            string
		precondition    func(t *testing.T)
		revisions       []string
		options         []RevlistOption
		expectedResults []RevlistResult
	}{
		{
			desc: "single blob",
			revisions: []string{
				lfsPointer1,
			},
			expectedResults: []RevlistResult{
				{OID: lfsPointer1},
			},
		},
		{
			desc: "multiple blobs",
			revisions: []string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
				lfsPointer4,
			},
			expectedResults: []RevlistResult{
				{OID: lfsPointer1},
				{OID: lfsPointer2},
				{OID: lfsPointer3},
				{OID: lfsPointer4},
			},
		},
		{
			desc: "duplicated blob prints blob once only",
			revisions: []string{
				lfsPointer1,
				lfsPointer1,
			},
			expectedResults: []RevlistResult{
				{OID: lfsPointer1},
			},
		},
		{
			desc: "tree results in object names",
			revisions: []string{
				"b95c0fad32f4361845f91d9ce4c1721b52b82793",
			},
			expectedResults: []RevlistResult{
				{OID: "b95c0fad32f4361845f91d9ce4c1721b52b82793"},
				{OID: "93e123ac8a3e6a0b600953d7598af629dec7b735", ObjectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "revision range",
			revisions: []string{
				"^refs/heads/master~",
				"refs/heads/master",
			},
			expectedResults: []RevlistResult{
				{OID: "1e292f8fedd741b75372e19097c76d327140c312"},
				{OID: "07f8147e8e73aab6c935c296e8cdc5194dee729b"},
				{OID: "ceb102b8d3f9a95c2eb979213e49f7cc1b23d56e", ObjectName: []byte("files")},
				{OID: "2132d150328bd9334cc4e62a16a5d998a7e399b9", ObjectName: []byte("files/flat")},
				{OID: "f3942dc8b824a2c9359e518d48e68f84461bd2f7", ObjectName: []byte("files/flat/path")},
				{OID: "ea7249055466085d0a6c69951908ef47757e92f4", ObjectName: []byte("files/flat/path/correct")},
				{OID: "c1c67abbaf91f624347bb3ae96eabe3a1b742478"},
			},
		},
		{
			// This is a tree object with multiple blobs. We cannot directly filter
			// blobs given that Git will always print whatever's been provided on the
			// command line. While we can already fix this with Git v2.32.0 via
			// the new `--filter-provided` option, let's defer this fix to a later
			// point. We demonstrate that this option is working by having the same test
			// twice, once without and once with limit.
			desc: "tree with multiple blobs without limit",
			revisions: []string{
				"79d5f98270ad677c86a7e1ab2baa922958565135",
			},
			expectedResults: []RevlistResult{
				{OID: "79d5f98270ad677c86a7e1ab2baa922958565135"},
				{OID: "8af7f880ce38649fc49f66e3f38857bfbec3f0b7", ObjectName: []byte("feature-1.txt")},
				{OID: "16ca0b267f82cd2f5ca1157dd162dae98745eab8", ObjectName: []byte("feature-2.txt")},
				{OID: "0fb47f093f769008049a0b0976ac3fa6d6125033", ObjectName: []byte("hotfix-1.txt")},
				{OID: "4ae6c5e14452a35d04156277ae63e8356eb17cae", ObjectName: []byte("hotfix-2.txt")},
				{OID: "b988ffed90cb6a9b7f98a3686a933edb3c5d70c0", ObjectName: []byte("iso8859.txt")},
				{OID: "570f8e1dfe8149c1d17002712310d43dfeb43159", ObjectName: []byte("russian.rb")},
				{OID: "7a17968582c21c9153ec24c6a9d5f33592ad9103", ObjectName: []byte("test.txt")},
				{OID: "f3064a3aa9c14277483f690250072e987e2c8356", ObjectName: []byte("\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88.txt")},
				{OID: "3a26c18b02e843b459732e7ade7ab9a154a1002b", ObjectName: []byte("\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88.xls")},
			},
		},
		{
			// And now the second time we execute this test with a limit and see that we
			// get less blobs as result.
			desc: "tree with multiple blobs with limit",
			revisions: []string{
				"79d5f98270ad677c86a7e1ab2baa922958565135",
			},
			options: []RevlistOption{
				WithBlobLimit(10),
			},
			expectedResults: []RevlistResult{
				{OID: "79d5f98270ad677c86a7e1ab2baa922958565135"},
				{OID: "0fb47f093f769008049a0b0976ac3fa6d6125033", ObjectName: []byte("hotfix-1.txt")},
				{OID: "4ae6c5e14452a35d04156277ae63e8356eb17cae", ObjectName: []byte("hotfix-2.txt")},
				{OID: "b988ffed90cb6a9b7f98a3686a933edb3c5d70c0", ObjectName: []byte("iso8859.txt")},
			},
		},
		{
			desc:         "tree with blob object type filter",
			precondition: needsObjectTypeFilters,
			revisions: []string{
				"79d5f98270ad677c86a7e1ab2baa922958565135",
			},
			options: []RevlistOption{
				WithObjectTypeFilter(ObjectTypeBlob),
			},
			expectedResults: []RevlistResult{
				{OID: "8af7f880ce38649fc49f66e3f38857bfbec3f0b7", ObjectName: []byte("feature-1.txt")},
				{OID: "16ca0b267f82cd2f5ca1157dd162dae98745eab8", ObjectName: []byte("feature-2.txt")},
				{OID: "0fb47f093f769008049a0b0976ac3fa6d6125033", ObjectName: []byte("hotfix-1.txt")},
				{OID: "4ae6c5e14452a35d04156277ae63e8356eb17cae", ObjectName: []byte("hotfix-2.txt")},
				{OID: "b988ffed90cb6a9b7f98a3686a933edb3c5d70c0", ObjectName: []byte("iso8859.txt")},
				{OID: "570f8e1dfe8149c1d17002712310d43dfeb43159", ObjectName: []byte("russian.rb")},
				{OID: "7a17968582c21c9153ec24c6a9d5f33592ad9103", ObjectName: []byte("test.txt")},
				{OID: "f3064a3aa9c14277483f690250072e987e2c8356", ObjectName: []byte("\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88.txt")},
				{OID: "3a26c18b02e843b459732e7ade7ab9a154a1002b", ObjectName: []byte("\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88.xls")},
			},
		},
		{
			desc:         "tree with tag object type filter",
			precondition: needsObjectTypeFilters,
			revisions: []string{
				"--all",
			},
			options: []RevlistOption{
				WithObjectTypeFilter(ObjectTypeTag),
			},
			expectedResults: []RevlistResult{
				{OID: "f4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8", ObjectName: []byte("v1.0.0")},
				{OID: "8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b", ObjectName: []byte("v1.1.0")},
				{OID: "8f03acbcd11c53d9c9468078f32a2622005a4841", ObjectName: []byte("v1.1.1")},
			},
		},
		{
			desc:         "tree with commit object type filter",
			precondition: needsObjectTypeFilters,
			revisions: []string{
				"79d5f98270ad677c86a7e1ab2baa922958565135",
			},
			options: []RevlistOption{
				WithObjectTypeFilter(ObjectTypeTree),
			},
			expectedResults: []RevlistResult{
				{OID: "79d5f98270ad677c86a7e1ab2baa922958565135"},
			},
		},
		{
			desc:         "tree with commit object type filter",
			precondition: needsObjectTypeFilters,
			revisions: []string{
				"^refs/heads/master~",
				"refs/heads/master",
			},
			options: []RevlistOption{
				WithObjectTypeFilter(ObjectTypeCommit),
			},
			expectedResults: []RevlistResult{
				{OID: "1e292f8fedd741b75372e19097c76d327140c312"},
				{OID: "c1c67abbaf91f624347bb3ae96eabe3a1b742478"},
			},
		},
		{
			desc:         "tree with object type and blob size filter",
			precondition: needsObjectTypeFilters,
			revisions: []string{
				"79d5f98270ad677c86a7e1ab2baa922958565135",
			},
			options: []RevlistOption{
				WithBlobLimit(10),
				WithObjectTypeFilter(ObjectTypeBlob),
			},
			expectedResults: []RevlistResult{
				{OID: "0fb47f093f769008049a0b0976ac3fa6d6125033", ObjectName: []byte("hotfix-1.txt")},
				{OID: "4ae6c5e14452a35d04156277ae63e8356eb17cae", ObjectName: []byte("hotfix-2.txt")},
				{OID: "b988ffed90cb6a9b7f98a3686a933edb3c5d70c0", ObjectName: []byte("iso8859.txt")},
			},
		},
		{
			desc: "invalid revision",
			revisions: []string{
				"refs/heads/does-not-exist",
			},
			expectedResults: []RevlistResult{
				{Err: errors.New("rev-list pipeline command: exit status 128")},
			},
		},
		{
			desc: "mixed valid and invalid revision",
			revisions: []string{
				lfsPointer1,
				"refs/heads/does-not-exist",
			},
			expectedResults: []RevlistResult{
				{Err: errors.New("rev-list pipeline command: exit status 128")},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.precondition != nil {
				tc.precondition(t)
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			resultChan := Revlist(ctx, repo, tc.revisions, tc.options...)

			var results []RevlistResult
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

func TestRevlistFilter(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		input           []RevlistResult
		filter          func(RevlistResult) bool
		expectedResults []RevlistResult
	}{
		{
			desc: "all accepted",
			input: []RevlistResult{
				{OID: "a"},
				{OID: "b"},
				{OID: "c"},
			},
			filter: func(RevlistResult) bool {
				return true
			},
			expectedResults: []RevlistResult{
				{OID: "a"},
				{OID: "b"},
				{OID: "c"},
			},
		},
		{
			desc: "all filtered",
			input: []RevlistResult{
				{OID: "a"},
				{OID: "b"},
				{OID: "c"},
			},
			filter: func(RevlistResult) bool {
				return false
			},
			expectedResults: nil,
		},
		{
			desc: "errors always get through",
			input: []RevlistResult{
				{OID: "a"},
				{OID: "b"},
				{Err: errors.New("foobar")},
				{OID: "c"},
			},
			filter: func(RevlistResult) bool {
				return false
			},
			expectedResults: []RevlistResult{
				{Err: errors.New("foobar")},
			},
		},
		{
			desc: "subset filtered",
			input: []RevlistResult{
				{OID: "a"},
				{OID: "b"},
				{OID: "c"},
			},
			filter: func(r RevlistResult) bool {
				return r.OID == "b"
			},
			expectedResults: []RevlistResult{
				{OID: "b"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			inputChan := make(chan RevlistResult, len(tc.input))
			for _, input := range tc.input {
				inputChan <- input
			}
			close(inputChan)

			var results []RevlistResult
			for result := range RevlistFilter(ctx, inputChan, tc.filter) {
				results = append(results, result)
			}

			require.Equal(t, tc.expectedResults, results)
		})
	}
}

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

			revlistResultChan := make(chan RevlistResult, len(tc.revlistInputs))
			for _, input := range tc.revlistInputs {
				revlistResultChan <- input
			}
			close(revlistResultChan)

			resultChan := CatfileInfo(ctx, catfileProcess, revlistResultChan)

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

func TestCatfileAllObjects(t *testing.T) {
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

func TestCatfileObject(t *testing.T) {
	cfg := testcfg.Build(t)

	repoProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	defer cleanup()
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	for _, tc := range []struct {
		desc              string
		catfileInfoInputs []CatfileInfoResult
		expectedResults   []CatfileObjectResult
	}{
		{
			desc: "single blob",
			catfileInfoInputs: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
			},
		},
		{
			desc: "multiple blobs",
			catfileInfoInputs: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer3, Type: "blob", Size: 127}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer4, Type: "blob", Size: 129}},
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer3, Type: "blob", Size: 127}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer4, Type: "blob", Size: 129}},
			},
		},
		{
			desc: "revlist result with object names",
			catfileInfoInputs: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793", Type: "tree", Size: 43}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}, ObjectName: []byte("branch-test.txt")},
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793", Type: "tree", Size: 43}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}, ObjectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "invalid object ID",
			catfileInfoInputs: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: "invalidobjectid", Type: "blob"}},
			},
			expectedResults: []CatfileObjectResult{
				{Err: errors.New("requesting object: object not found")},
			},
		},
		{
			desc: "invalid object type",
			catfileInfoInputs: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "foobar"}},
			},
			expectedResults: []CatfileObjectResult{
				{Err: errors.New("requesting object: unknown object type \"foobar\"")},
			},
		},
		{
			desc: "mixed valid and invalid revision",
			catfileInfoInputs: []CatfileInfoResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "foobar"}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer2}},
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{Err: errors.New("requesting object: unknown object type \"foobar\"")},
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

			catfileInfoResultChan := make(chan CatfileInfoResult, len(tc.catfileInfoInputs))
			for _, input := range tc.catfileInfoInputs {
				catfileInfoResultChan <- input
			}
			close(catfileInfoResultChan)

			resultChan := CatfileObject(ctx, catfileProcess, catfileInfoResultChan)

			var results []CatfileObjectResult
			for result := range resultChan {
				// We're converting the error here to a plain un-nested error such
				// that we don't have to replicate the complete error's structure.
				if result.Err != nil {
					result.Err = errors.New(result.Err.Error())
				}

				if result.Err == nil {
					// While we could also assert object data, let's not do
					// this: it would just be too annoying.
					require.NotNil(t, result.ObjectReader)

					objectData, err := ioutil.ReadAll(result.ObjectReader)
					require.NoError(t, err)
					require.Len(t, objectData, int(result.ObjectInfo.Size))

					result.ObjectReader = nil
				}

				results = append(results, result)
			}

			require.Equal(t, tc.expectedResults, results)
		})
	}
}

func TestPipeline(t *testing.T) {
	cfg := testcfg.Build(t)

	repoProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	defer cleanup()
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	for _, tc := range []struct {
		desc              string
		revisions         []string
		revlistFilter     func(RevlistResult) bool
		catfileInfoFilter func(CatfileInfoResult) bool
		expectedResults   []CatfileObjectResult
	}{
		{
			desc: "single blob",
			revisions: []string{
				lfsPointer1,
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
			},
		},
		{
			desc: "multiple blobs",
			revisions: []string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer3, Type: "blob", Size: 127}},
			},
		},
		{
			desc: "multiple blobs with filter",
			revisions: []string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
			},
			revlistFilter: func(r RevlistResult) bool {
				return r.OID == lfsPointer2
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
			},
		},
		{
			desc: "tree",
			revisions: []string{
				"b95c0fad32f4361845f91d9ce4c1721b52b82793",
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793", Type: "tree", Size: 43}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}, ObjectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "tree with blob filter",
			revisions: []string{
				"b95c0fad32f4361845f91d9ce4c1721b52b82793",
			},
			catfileInfoFilter: func(r CatfileInfoResult) bool {
				return r.ObjectInfo.Type == "blob"
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}, ObjectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "revision range",
			revisions: []string{
				"^master~",
				"master",
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: "1e292f8fedd741b75372e19097c76d327140c312", Type: "commit", Size: 388}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: "07f8147e8e73aab6c935c296e8cdc5194dee729b", Type: "tree", Size: 780}},
				{ObjectInfo: &catfile.ObjectInfo{Oid: "ceb102b8d3f9a95c2eb979213e49f7cc1b23d56e", Type: "tree", Size: 258}, ObjectName: []byte("files")},
				{ObjectInfo: &catfile.ObjectInfo{Oid: "2132d150328bd9334cc4e62a16a5d998a7e399b9", Type: "tree", Size: 31}, ObjectName: []byte("files/flat")},
				{ObjectInfo: &catfile.ObjectInfo{Oid: "f3942dc8b824a2c9359e518d48e68f84461bd2f7", Type: "tree", Size: 34}, ObjectName: []byte("files/flat/path")},
				{ObjectInfo: &catfile.ObjectInfo{Oid: "ea7249055466085d0a6c69951908ef47757e92f4", Type: "tree", Size: 39}, ObjectName: []byte("files/flat/path/correct")},
				{ObjectInfo: &catfile.ObjectInfo{Oid: "c1c67abbaf91f624347bb3ae96eabe3a1b742478", Type: "commit", Size: 326}},
			},
		},
		{
			desc: "--all with all filters",
			revisions: []string{
				"--all",
			},
			revlistFilter: func(r RevlistResult) bool {
				// Let through two LFS pointers and a tree.
				return r.OID == "b95c0fad32f4361845f91d9ce4c1721b52b82793" ||
					r.OID == lfsPointer1 || r.OID == lfsPointer2
			},
			catfileInfoFilter: func(r CatfileInfoResult) bool {
				// Only let through blobs, so only the two LFS pointers remain.
				return r.ObjectInfo.Type == "blob"
			},
			expectedResults: []CatfileObjectResult{
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}, ObjectName: []byte("files/lfs/lfs_object.iso")},
				{ObjectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}, ObjectName: []byte("another.lfs")},
			},
		},
		{
			desc: "invalid revision",
			revisions: []string{
				"doesnotexist",
			},
			expectedResults: []CatfileObjectResult{
				{Err: errors.New("rev-list pipeline command: exit status 128")},
			},
		},
		{
			desc: "mixed valid and invalid revision",
			revisions: []string{
				lfsPointer1,
				"doesnotexist",
				lfsPointer2,
			},
			expectedResults: []CatfileObjectResult{
				{Err: errors.New("rev-list pipeline command: exit status 128")},
			},
		},
		{
			desc: "invalid revision with all filters",
			revisions: []string{
				"doesnotexist",
			},
			revlistFilter: func(r RevlistResult) bool {
				require.Fail(t, "filter should not be invoked on errors")
				return true
			},
			catfileInfoFilter: func(r CatfileInfoResult) bool {
				require.Fail(t, "filter should not be invoked on errors")
				return true
			},
			expectedResults: []CatfileObjectResult{
				{Err: errors.New("rev-list pipeline command: exit status 128")},
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

			revlistChan := Revlist(ctx, repo, tc.revisions)
			if tc.revlistFilter != nil {
				revlistChan = RevlistFilter(ctx, revlistChan, tc.revlistFilter)
			}

			catfileInfoChan := CatfileInfo(ctx, catfileProcess, revlistChan)
			if tc.catfileInfoFilter != nil {
				catfileInfoChan = CatfileInfoFilter(ctx, catfileInfoChan, tc.catfileInfoFilter)
			}

			catfileObjectChan := CatfileObject(ctx, catfileProcess, catfileInfoChan)

			var results []CatfileObjectResult
			for result := range catfileObjectChan {
				// We're converting the error here to a plain un-nested error such
				// that we don't have to replicate the complete error's structure.
				if result.Err != nil {
					result.Err = errors.New(result.Err.Error())
				}

				if result.Err == nil {
					// While we could also assert object data, let's not do
					// this: it would just be too annoying.
					require.NotNil(t, result.ObjectReader)

					objectData, err := ioutil.ReadAll(result.ObjectReader)
					require.NoError(t, err)
					require.Len(t, objectData, int(result.ObjectInfo.Size))

					result.ObjectReader = nil
				}

				results = append(results, result)
			}

			require.Equal(t, tc.expectedResults, results)
		})
	}

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		catfileCache := catfile.NewCache(cfg)
		defer catfileCache.Stop()

		catfileProcess, err := catfileCache.BatchProcess(ctx, repo)
		require.NoError(t, err)

		// We need to create a separate child context because otherwise we'd kill the batch
		// process.
		childCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		revlistChan := Revlist(childCtx, repo, []string{"--all"})
		revlistChan = RevlistFilter(childCtx, revlistChan, func(RevlistResult) bool { return true })
		catfileInfoChan := CatfileInfo(childCtx, catfileProcess, revlistChan)
		catfileInfoChan = CatfileInfoFilter(childCtx, catfileInfoChan, func(CatfileInfoResult) bool { return true })
		catfileObjectChan := CatfileObject(childCtx, catfileProcess, catfileInfoChan)

		i := 0
		for result := range catfileObjectChan {
			require.NoError(t, result.Err)
			i++

			_, err := io.Copy(ioutil.Discard, result.ObjectReader)
			require.NoError(t, err)

			if i == 3 {
				cancel()
			}
		}

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

		catfileProcess, err := catfileCache.BatchProcess(ctx, repo)
		require.NoError(t, err)

		revlistChan := Revlist(ctx, repo, []string{"--all"})
		catfileInfoChan := CatfileInfo(ctx, catfileProcess, revlistChan)
		catfileObjectChan := CatfileObject(ctx, catfileProcess, catfileInfoChan)

		i := 0
		var wg sync.WaitGroup
		for result := range catfileObjectChan {
			require.NoError(t, result.Err)

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
				_, err := io.Copy(ioutil.Discard, object.ObjectReader)
				require.NoError(t, err)
			}(result)
		}

		wg.Wait()

		// We could in theory assert the exact amount of objects, but this would make it
		// harder than necessary to change the test repo's contents.
		require.Greater(t, i, 1000)
	})
}
