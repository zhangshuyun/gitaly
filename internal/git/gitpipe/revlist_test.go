package gitpipe

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
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
		expectedErr     error
	}{
		{
			desc: "single blob",
			revisions: []string{
				lfsPointer1,
			},
			options: []RevlistOption{
				WithObjects(),
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
			options: []RevlistOption{
				WithObjects(),
			},
			expectedResults: []RevlistResult{
				{OID: lfsPointer1},
				{OID: lfsPointer2},
				{OID: lfsPointer3},
				{OID: lfsPointer4},
			},
		},
		{
			desc: "multiple blobs without objects",
			revisions: []string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
				lfsPointer4,
			},
			expectedResults: nil,
		},
		{
			desc: "duplicated blob prints blob once only",
			revisions: []string{
				lfsPointer1,
				lfsPointer1,
			},
			options: []RevlistOption{
				WithObjects(),
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
			options: []RevlistOption{
				WithObjects(),
			},
			expectedResults: []RevlistResult{
				{OID: "b95c0fad32f4361845f91d9ce4c1721b52b82793"},
				{OID: "93e123ac8a3e6a0b600953d7598af629dec7b735", ObjectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "tree without objects returns nothing",
			revisions: []string{
				"b95c0fad32f4361845f91d9ce4c1721b52b82793",
			},
			expectedResults: nil,
		},
		{
			desc: "revision without disabled walk",
			revisions: []string{
				"refs/heads/master",
			},
			options: []RevlistOption{
				WithDisabledWalk(),
			},
			expectedResults: []RevlistResult{
				{OID: "1e292f8fedd741b75372e19097c76d327140c312"},
			},
		},
		{
			desc: "revision range",
			revisions: []string{
				"^refs/heads/master~",
				"refs/heads/master",
			},
			options: []RevlistOption{
				WithObjects(),
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
			desc: "revision range without objects",
			revisions: []string{
				"^refs/heads/master~",
				"refs/heads/master",
			},
			expectedResults: []RevlistResult{
				{OID: "1e292f8fedd741b75372e19097c76d327140c312"},
				{OID: "c1c67abbaf91f624347bb3ae96eabe3a1b742478"},
			},
		},
		{
			desc: "revision range without objects with at most one parent",
			revisions: []string{
				"^refs/heads/master~",
				"refs/heads/master",
			},
			options: []RevlistOption{
				WithMaxParents(1),
			},
			expectedResults: []RevlistResult{
				{OID: "c1c67abbaf91f624347bb3ae96eabe3a1b742478"},
			},
		},
		{
			desc: "revision range with topo order",
			revisions: []string{
				// This is one of the smaller examples I've found which reproduces
				// different sorting orders between topo- and date-sorting. Expected
				// results contain the same object for this and the next test case,
				// but ordering is different.
				"master",
				"^master~5",
				"flat-path",
			},
			options: []RevlistOption{
				WithOrder(OrderTopo),
			},
			expectedResults: []RevlistResult{
				{OID: "1e292f8fedd741b75372e19097c76d327140c312"},
				{OID: "c1c67abbaf91f624347bb3ae96eabe3a1b742478"},
				{OID: "7975be0116940bf2ad4321f79d02a55c5f7779aa"},
				{OID: "c84ff944ff4529a70788a5e9003c2b7feae29047"},
				{OID: "60ecb67744cb56576c30214ff52294f8ce2def98"},
				{OID: "55bc176024cfa3baaceb71db584c7e5df900ea65"},
				{OID: "e63f41fe459e62e1228fcef60d7189127aeba95a"},
				{OID: "4a24d82dbca5c11c61556f3b35ca472b7463187e"},
				{OID: "b83d6e391c22777fca1ed3012fce84f633d7fed0"},
				{OID: "498214de67004b1da3d820901307bed2a68a8ef6"},
				// The following commit is sorted differently in the next testcase.
				{OID: "ce369011c189f62c815f5971d096b26759bab0d1"},
			},
		},
		{
			desc: "revision range with date order",
			revisions: []string{
				"master",
				"^master~5",
				"flat-path",
			},
			options: []RevlistOption{
				WithOrder(OrderDate),
			},
			expectedResults: []RevlistResult{
				{OID: "1e292f8fedd741b75372e19097c76d327140c312"},
				{OID: "c1c67abbaf91f624347bb3ae96eabe3a1b742478"},
				{OID: "7975be0116940bf2ad4321f79d02a55c5f7779aa"},
				{OID: "c84ff944ff4529a70788a5e9003c2b7feae29047"},
				{OID: "60ecb67744cb56576c30214ff52294f8ce2def98"},
				{OID: "55bc176024cfa3baaceb71db584c7e5df900ea65"},
				// The following commit is sorted differently in the previous
				// testcase.
				{OID: "ce369011c189f62c815f5971d096b26759bab0d1"},
				{OID: "e63f41fe459e62e1228fcef60d7189127aeba95a"},
				{OID: "4a24d82dbca5c11c61556f3b35ca472b7463187e"},
				{OID: "b83d6e391c22777fca1ed3012fce84f633d7fed0"},
				{OID: "498214de67004b1da3d820901307bed2a68a8ef6"},
			},
		},
		{
			desc: "revision range with dates",
			revisions: []string{
				"refs/heads/master",
			},
			options: []RevlistOption{
				WithBefore(time.Date(2016, 6, 30, 18, 30, 0, 0, time.UTC)),
				WithAfter(time.Date(2016, 6, 30, 18, 28, 0, 0, time.UTC)),
			},
			expectedResults: []RevlistResult{
				{OID: "6907208d755b60ebeacb2e9dfea74c92c3449a1f"},
				{OID: "c347ca2e140aa667b968e51ed0ffe055501fe4f4"},
			},
		},
		{
			desc: "revision range with author",
			revisions: []string{
				"refs/heads/master",
			},
			options: []RevlistOption{
				WithAuthor([]byte("Sytse")),
			},
			expectedResults: []RevlistResult{
				{OID: "e56497bb5f03a90a51293fc6d516788730953899"},
			},
		},
		{
			desc: "first parent chain",
			revisions: []string{
				"master",
				"^master~4",
			},
			options: []RevlistOption{
				WithFirstParent(),
			},
			expectedResults: []RevlistResult{
				{OID: "1e292f8fedd741b75372e19097c76d327140c312"},
				{OID: "7975be0116940bf2ad4321f79d02a55c5f7779aa"},
				{OID: "60ecb67744cb56576c30214ff52294f8ce2def98"},
				{OID: "e63f41fe459e62e1228fcef60d7189127aeba95a"},
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
			options: []RevlistOption{
				WithObjects(),
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
				WithObjects(),
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
				WithObjects(),
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
				WithObjects(),
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
				WithObjects(),
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
				WithObjects(),
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
				WithObjects(),
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
			expectedErr: errors.New("rev-list pipeline command: exit status 128"),
		},
		{
			desc: "mixed valid and invalid revision",
			revisions: []string{
				lfsPointer1,
				"refs/heads/does-not-exist",
			},
			expectedErr: errors.New("rev-list pipeline command: exit status 128"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if tc.precondition != nil {
				tc.precondition(t)
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			it := Revlist(ctx, repo, tc.revisions, tc.options...)

			var results []RevlistResult
			for it.Next() {
				results = append(results, it.Result())
			}

			// We're converting the error here to a plain un-nested error such that we
			// don't have to replicate the complete error's structure.
			err := it.Err()
			if err != nil {
				err = errors.New(err.Error())
			}

			require.Equal(t, tc.expectedErr, err)
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
		expectedErr     error
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
				{err: errors.New("foobar")},
				{OID: "c"},
			},
			filter: func(RevlistResult) bool {
				return false
			},
			expectedErr: errors.New("foobar"),
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

			it := RevlistFilter(ctx, NewRevlistIterator(tc.input), tc.filter)

			var results []RevlistResult
			for it.Next() {
				results = append(results, it.Result())
			}

			require.Equal(t, tc.expectedErr, it.Err())
			require.Equal(t, tc.expectedResults, results)
		})
	}
}
