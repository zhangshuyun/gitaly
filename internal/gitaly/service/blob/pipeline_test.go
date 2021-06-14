package blob

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
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

	for _, tc := range []struct {
		desc            string
		revisions       []string
		options         []revlistOption
		expectedResults []revlistResult
	}{
		{
			desc: "single blob",
			revisions: []string{
				lfsPointer1,
			},
			expectedResults: []revlistResult{
				{oid: lfsPointer1},
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
			expectedResults: []revlistResult{
				{oid: lfsPointer1},
				{oid: lfsPointer2},
				{oid: lfsPointer3},
				{oid: lfsPointer4},
			},
		},
		{
			desc: "duplicated blob prints blob once only",
			revisions: []string{
				lfsPointer1,
				lfsPointer1,
			},
			expectedResults: []revlistResult{
				{oid: lfsPointer1},
			},
		},
		{
			desc: "tree results in object names",
			revisions: []string{
				"b95c0fad32f4361845f91d9ce4c1721b52b82793",
			},
			expectedResults: []revlistResult{
				{oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793"},
				{oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", objectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "revision range",
			revisions: []string{
				"^refs/heads/master~",
				"refs/heads/master",
			},
			expectedResults: []revlistResult{
				{oid: "1e292f8fedd741b75372e19097c76d327140c312"},
				{oid: "07f8147e8e73aab6c935c296e8cdc5194dee729b"},
				{oid: "ceb102b8d3f9a95c2eb979213e49f7cc1b23d56e", objectName: []byte("files")},
				{oid: "2132d150328bd9334cc4e62a16a5d998a7e399b9", objectName: []byte("files/flat")},
				{oid: "f3942dc8b824a2c9359e518d48e68f84461bd2f7", objectName: []byte("files/flat/path")},
				{oid: "ea7249055466085d0a6c69951908ef47757e92f4", objectName: []byte("files/flat/path/correct")},
				{oid: "c1c67abbaf91f624347bb3ae96eabe3a1b742478"},
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
			expectedResults: []revlistResult{
				{oid: "79d5f98270ad677c86a7e1ab2baa922958565135"},
				{oid: "8af7f880ce38649fc49f66e3f38857bfbec3f0b7", objectName: []byte("feature-1.txt")},
				{oid: "16ca0b267f82cd2f5ca1157dd162dae98745eab8", objectName: []byte("feature-2.txt")},
				{oid: "0fb47f093f769008049a0b0976ac3fa6d6125033", objectName: []byte("hotfix-1.txt")},
				{oid: "4ae6c5e14452a35d04156277ae63e8356eb17cae", objectName: []byte("hotfix-2.txt")},
				{oid: "b988ffed90cb6a9b7f98a3686a933edb3c5d70c0", objectName: []byte("iso8859.txt")},
				{oid: "570f8e1dfe8149c1d17002712310d43dfeb43159", objectName: []byte("russian.rb")},
				{oid: "7a17968582c21c9153ec24c6a9d5f33592ad9103", objectName: []byte("test.txt")},
				{oid: "f3064a3aa9c14277483f690250072e987e2c8356", objectName: []byte("\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88.txt")},
				{oid: "3a26c18b02e843b459732e7ade7ab9a154a1002b", objectName: []byte("\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88.xls")},
			},
		},
		{
			// And now the second time we execute this test with a limit and see that we
			// get less blobs as result.
			desc: "tree with multiple blobs with limit",
			revisions: []string{
				"79d5f98270ad677c86a7e1ab2baa922958565135",
			},
			options: []revlistOption{
				withBlobLimit(10),
			},
			expectedResults: []revlistResult{
				{oid: "79d5f98270ad677c86a7e1ab2baa922958565135"},
				{oid: "0fb47f093f769008049a0b0976ac3fa6d6125033", objectName: []byte("hotfix-1.txt")},
				{oid: "4ae6c5e14452a35d04156277ae63e8356eb17cae", objectName: []byte("hotfix-2.txt")},
				{oid: "b988ffed90cb6a9b7f98a3686a933edb3c5d70c0", objectName: []byte("iso8859.txt")},
			},
		},
		{
			desc: "invalid revision",
			revisions: []string{
				"refs/heads/does-not-exist",
			},
			expectedResults: []revlistResult{
				{err: errors.New("rev-list pipeline command: exit status 128")},
			},
		},
		{
			desc: "mixed valid and invalid revision",
			revisions: []string{
				lfsPointer1,
				"refs/heads/does-not-exist",
			},
			expectedResults: []revlistResult{
				{err: errors.New("rev-list pipeline command: exit status 128")},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			resultChan := revlist(ctx, repo, tc.revisions, tc.options...)

			var results []revlistResult
			for result := range resultChan {
				// We're converting the error here to a plain un-nested error such
				// that we don't have to replicate the complete error's structure.
				if result.err != nil {
					result.err = errors.New(result.err.Error())
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
		input           []revlistResult
		filter          func(revlistResult) bool
		expectedResults []revlistResult
	}{
		{
			desc: "all accepted",
			input: []revlistResult{
				{oid: "a"},
				{oid: "b"},
				{oid: "c"},
			},
			filter: func(revlistResult) bool {
				return true
			},
			expectedResults: []revlistResult{
				{oid: "a"},
				{oid: "b"},
				{oid: "c"},
			},
		},
		{
			desc: "all filtered",
			input: []revlistResult{
				{oid: "a"},
				{oid: "b"},
				{oid: "c"},
			},
			filter: func(revlistResult) bool {
				return false
			},
			expectedResults: nil,
		},
		{
			desc: "errors always get through",
			input: []revlistResult{
				{oid: "a"},
				{oid: "b"},
				{err: errors.New("foobar")},
				{oid: "c"},
			},
			filter: func(revlistResult) bool {
				return false
			},
			expectedResults: []revlistResult{
				{err: errors.New("foobar")},
			},
		},
		{
			desc: "subset filtered",
			input: []revlistResult{
				{oid: "a"},
				{oid: "b"},
				{oid: "c"},
			},
			filter: func(r revlistResult) bool {
				return r.oid == "b"
			},
			expectedResults: []revlistResult{
				{oid: "b"},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			inputChan := make(chan revlistResult, len(tc.input))
			for _, input := range tc.input {
				inputChan <- input
			}
			close(inputChan)

			var results []revlistResult
			for result := range revlistFilter(ctx, inputChan, tc.filter) {
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
		revlistInputs   []revlistResult
		expectedResults []catfileInfoResult
	}{
		{
			desc: "single blob",
			revlistInputs: []revlistResult{
				{oid: lfsPointer1},
			},
			expectedResults: []catfileInfoResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
			},
		},
		{
			desc: "multiple blobs",
			revlistInputs: []revlistResult{
				{oid: lfsPointer1},
				{oid: lfsPointer2},
				{oid: lfsPointer3},
				{oid: lfsPointer4},
			},
			expectedResults: []catfileInfoResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer3, Type: "blob", Size: 127}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer4, Type: "blob", Size: 129}},
			},
		},
		{
			desc: "object name",
			revlistInputs: []revlistResult{
				{oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793"},
				{oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", objectName: []byte("branch-test.txt")},
			},
			expectedResults: []catfileInfoResult{
				{objectInfo: &catfile.ObjectInfo{Oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793", Type: "tree", Size: 43}},
				{objectInfo: &catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}, objectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "invalid object ID",
			revlistInputs: []revlistResult{
				{oid: "invalidobjectid"},
			},
			expectedResults: []catfileInfoResult{
				{err: errors.New("retrieving object info for \"invalidobjectid\": object not found")},
			},
		},
		{
			desc: "mixed valid and invalid revision",
			revlistInputs: []revlistResult{
				{oid: lfsPointer1},
				{oid: "invalidobjectid"},
				{oid: lfsPointer2},
			},
			expectedResults: []catfileInfoResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{err: errors.New("retrieving object info for \"invalidobjectid\": object not found")},
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

			revlistResultChan := make(chan revlistResult, len(tc.revlistInputs))
			for _, input := range tc.revlistInputs {
				revlistResultChan <- input
			}
			close(revlistResultChan)

			resultChan := catfileInfo(ctx, catfileProcess, revlistResultChan)

			var results []catfileInfoResult
			for result := range resultChan {
				// We're converting the error here to a plain un-nested error such
				// that we don't have to replicate the complete error's structure.
				if result.err != nil {
					result.err = errors.New(result.err.Error())
				}

				results = append(results, result)
			}

			require.Equal(t, tc.expectedResults, results)
		})
	}
}

func TestCatfileInfoFilter(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		input           []catfileInfoResult
		filter          func(catfileInfoResult) bool
		expectedResults []catfileInfoResult
	}{
		{
			desc: "all accepted",
			input: []catfileInfoResult{
				{objectName: []byte{'a'}},
				{objectName: []byte{'b'}},
				{objectName: []byte{'c'}},
			},
			filter: func(catfileInfoResult) bool {
				return true
			},
			expectedResults: []catfileInfoResult{
				{objectName: []byte{'a'}},
				{objectName: []byte{'b'}},
				{objectName: []byte{'c'}},
			},
		},
		{
			desc: "all filtered",
			input: []catfileInfoResult{
				{objectName: []byte{'a'}},
				{objectName: []byte{'b'}},
				{objectName: []byte{'c'}},
			},
			filter: func(catfileInfoResult) bool {
				return false
			},
		},
		{
			desc: "errors always get through",
			input: []catfileInfoResult{
				{objectName: []byte{'a'}},
				{objectName: []byte{'b'}},
				{err: errors.New("foobar")},
				{objectName: []byte{'c'}},
			},
			filter: func(catfileInfoResult) bool {
				return false
			},
			expectedResults: []catfileInfoResult{
				{err: errors.New("foobar")},
			},
		},
		{
			desc: "subset filtered",
			input: []catfileInfoResult{
				{objectName: []byte{'a'}},
				{objectName: []byte{'b'}},
				{objectName: []byte{'c'}},
			},
			filter: func(r catfileInfoResult) bool {
				return r.objectName[0] == 'b'
			},
			expectedResults: []catfileInfoResult{
				{objectName: []byte{'b'}},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			inputChan := make(chan catfileInfoResult, len(tc.input))
			for _, input := range tc.input {
				inputChan <- input
			}
			close(inputChan)

			var results []catfileInfoResult
			for result := range catfileInfoFilter(ctx, inputChan, tc.filter) {
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
		catfileInfoInputs []catfileInfoResult
		expectedResults   []catfileObjectResult
	}{
		{
			desc: "single blob",
			catfileInfoInputs: []catfileInfoResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
			},
		},
		{
			desc: "multiple blobs",
			catfileInfoInputs: []catfileInfoResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer3, Type: "blob", Size: 127}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer4, Type: "blob", Size: 129}},
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer3, Type: "blob", Size: 127}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer4, Type: "blob", Size: 129}},
			},
		},
		{
			desc: "revlist result with object names",
			catfileInfoInputs: []catfileInfoResult{
				{objectInfo: &catfile.ObjectInfo{Oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793", Type: "tree", Size: 43}},
				{objectInfo: &catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}, objectName: []byte("branch-test.txt")},
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793", Type: "tree", Size: 43}},
				{objectInfo: &catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}, objectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "invalid object ID",
			catfileInfoInputs: []catfileInfoResult{
				{objectInfo: &catfile.ObjectInfo{Oid: "invalidobjectid", Type: "blob"}},
			},
			expectedResults: []catfileObjectResult{
				{err: errors.New("requesting object: object not found")},
			},
		},
		{
			desc: "invalid object type",
			catfileInfoInputs: []catfileInfoResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "foobar"}},
			},
			expectedResults: []catfileObjectResult{
				{err: errors.New("requesting object: unknown object type \"foobar\"")},
			},
		},
		{
			desc: "mixed valid and invalid revision",
			catfileInfoInputs: []catfileInfoResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "foobar"}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer2}},
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{err: errors.New("requesting object: unknown object type \"foobar\"")},
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

			catfileInfoResultChan := make(chan catfileInfoResult, len(tc.catfileInfoInputs))
			for _, input := range tc.catfileInfoInputs {
				catfileInfoResultChan <- input
			}
			close(catfileInfoResultChan)

			resultChan := catfileObject(ctx, catfileProcess, catfileInfoResultChan)

			var results []catfileObjectResult
			for result := range resultChan {
				// We're converting the error here to a plain un-nested error such
				// that we don't have to replicate the complete error's structure.
				if result.err != nil {
					result.err = errors.New(result.err.Error())
				}

				if result.err == nil {
					// While we could also assert object data, let's not do this: it
					// would just be too annoying.
					require.NotNil(t, result.objectData)
					require.Len(t, result.objectData, int(result.objectInfo.Size))
					result.objectData = nil
				}

				results = append(results, result)
			}

			require.Equal(t, tc.expectedResults, results)
		})
	}
}

func TestCatfileObjectFilter(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		input           []catfileObjectResult
		filter          func(catfileObjectResult) bool
		expectedResults []catfileObjectResult
	}{
		{
			desc: "all accepted",
			input: []catfileObjectResult{
				{objectName: []byte{'a'}},
				{objectName: []byte{'b'}},
				{objectName: []byte{'c'}},
			},
			filter: func(catfileObjectResult) bool {
				return true
			},
			expectedResults: []catfileObjectResult{
				{objectName: []byte{'a'}},
				{objectName: []byte{'b'}},
				{objectName: []byte{'c'}},
			},
		},
		{
			desc: "all filtered",
			input: []catfileObjectResult{
				{objectName: []byte{'a'}},
				{objectName: []byte{'b'}},
				{objectName: []byte{'c'}},
			},
			filter: func(catfileObjectResult) bool {
				return false
			},
		},
		{
			desc: "errors always get through",
			input: []catfileObjectResult{
				{objectName: []byte{'a'}},
				{objectName: []byte{'b'}},
				{err: errors.New("foobar")},
				{objectName: []byte{'c'}},
			},
			filter: func(catfileObjectResult) bool {
				return false
			},
			expectedResults: []catfileObjectResult{
				{err: errors.New("foobar")},
			},
		},
		{
			desc: "subset filtered",
			input: []catfileObjectResult{
				{objectName: []byte{'a'}},
				{objectName: []byte{'b'}},
				{objectName: []byte{'c'}},
			},
			filter: func(r catfileObjectResult) bool {
				return r.objectName[0] == 'b'
			},
			expectedResults: []catfileObjectResult{
				{objectName: []byte{'b'}},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			inputChan := make(chan catfileObjectResult, len(tc.input))
			for _, input := range tc.input {
				inputChan <- input
			}
			close(inputChan)

			var results []catfileObjectResult
			for result := range catfileObjectFilter(ctx, inputChan, tc.filter) {
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
		desc                string
		revisions           []string
		revlistFilter       func(revlistResult) bool
		catfileInfoFilter   func(catfileInfoResult) bool
		catfileObjectFilter func(catfileObjectResult) bool
		expectedResults     []catfileObjectResult
	}{
		{
			desc: "single blob",
			revisions: []string{
				lfsPointer1,
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
			},
		},
		{
			desc: "multiple blobs",
			revisions: []string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer3, Type: "blob", Size: 127}},
			},
		},
		{
			desc: "multiple blobs with filter",
			revisions: []string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
			},
			revlistFilter: func(r revlistResult) bool {
				return r.oid == lfsPointer2
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer2, Type: "blob", Size: 127}},
			},
		},
		{
			desc: "tree",
			revisions: []string{
				"b95c0fad32f4361845f91d9ce4c1721b52b82793",
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: "b95c0fad32f4361845f91d9ce4c1721b52b82793", Type: "tree", Size: 43}},
				{objectInfo: &catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}, objectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "tree with blob filter",
			revisions: []string{
				"b95c0fad32f4361845f91d9ce4c1721b52b82793",
			},
			catfileInfoFilter: func(r catfileInfoResult) bool {
				return r.objectInfo.Type == "blob"
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: "93e123ac8a3e6a0b600953d7598af629dec7b735", Type: "blob", Size: 59}, objectName: []byte("branch-test.txt")},
			},
		},
		{
			desc: "revision range",
			revisions: []string{
				"^master~",
				"master",
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: "1e292f8fedd741b75372e19097c76d327140c312", Type: "commit", Size: 388}},
				{objectInfo: &catfile.ObjectInfo{Oid: "07f8147e8e73aab6c935c296e8cdc5194dee729b", Type: "tree", Size: 780}},
				{objectInfo: &catfile.ObjectInfo{Oid: "ceb102b8d3f9a95c2eb979213e49f7cc1b23d56e", Type: "tree", Size: 258}, objectName: []byte("files")},
				{objectInfo: &catfile.ObjectInfo{Oid: "2132d150328bd9334cc4e62a16a5d998a7e399b9", Type: "tree", Size: 31}, objectName: []byte("files/flat")},
				{objectInfo: &catfile.ObjectInfo{Oid: "f3942dc8b824a2c9359e518d48e68f84461bd2f7", Type: "tree", Size: 34}, objectName: []byte("files/flat/path")},
				{objectInfo: &catfile.ObjectInfo{Oid: "ea7249055466085d0a6c69951908ef47757e92f4", Type: "tree", Size: 39}, objectName: []byte("files/flat/path/correct")},
				{objectInfo: &catfile.ObjectInfo{Oid: "c1c67abbaf91f624347bb3ae96eabe3a1b742478", Type: "commit", Size: 326}},
			},
		},
		{
			desc: "revision with blob contents filter",
			revisions: []string{
				"master",
			},
			catfileObjectFilter: func(r catfileObjectResult) bool {
				return bytes.HasPrefix(r.objectData, []byte("/custom-highlighting/"))
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: "b680596c9f3a3c834b933aef14f94a0ab9fa604a", Type: "blob", Size: 100}, objectName: []byte(".gitattributes")},
				{objectInfo: &catfile.ObjectInfo{Oid: "36814a3da051159a1683479e7a1487120309db8f", Type: "blob", Size: 58}, objectName: []byte(".gitattributes")},
			},
		},
		{
			desc: "--all with all filters",
			revisions: []string{
				"--all",
			},
			revlistFilter: func(r revlistResult) bool {
				// Let through two LFS pointers and a tree.
				return r.oid == "b95c0fad32f4361845f91d9ce4c1721b52b82793" ||
					r.oid == lfsPointer1 || r.oid == lfsPointer2
			},
			catfileInfoFilter: func(r catfileInfoResult) bool {
				// Only let through blobs, so only the two LFS pointers remain.
				return r.objectInfo.Type == "blob"
			},
			catfileObjectFilter: func(r catfileObjectResult) bool {
				// This brings it down to a single LFS pointer.
				return len(r.objectData) == 133
			},
			expectedResults: []catfileObjectResult{
				{objectInfo: &catfile.ObjectInfo{Oid: lfsPointer1, Type: "blob", Size: 133}, objectName: []byte("files/lfs/lfs_object.iso")},
			},
		},
		{
			desc: "invalid revision",
			revisions: []string{
				"doesnotexist",
			},
			expectedResults: []catfileObjectResult{
				{err: errors.New("rev-list pipeline command: exit status 128")},
			},
		},
		{
			desc: "mixed valid and invalid revision",
			revisions: []string{
				lfsPointer1,
				"doesnotexist",
				lfsPointer2,
			},
			expectedResults: []catfileObjectResult{
				{err: errors.New("rev-list pipeline command: exit status 128")},
			},
		},
		{
			desc: "invalid revision with all filters",
			revisions: []string{
				"doesnotexist",
			},
			revlistFilter: func(r revlistResult) bool {
				require.Fail(t, "filter should not be invoked on errors")
				return true
			},
			catfileInfoFilter: func(r catfileInfoResult) bool {
				require.Fail(t, "filter should not be invoked on errors")
				return true
			},
			catfileObjectFilter: func(r catfileObjectResult) bool {
				require.Fail(t, "filter should not be invoked on errors")
				return true
			},
			expectedResults: []catfileObjectResult{
				{err: errors.New("rev-list pipeline command: exit status 128")},
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

			revlistChan := revlist(ctx, repo, tc.revisions)
			if tc.revlistFilter != nil {
				revlistChan = revlistFilter(ctx, revlistChan, tc.revlistFilter)
			}

			catfileInfoChan := catfileInfo(ctx, catfileProcess, revlistChan)
			if tc.catfileInfoFilter != nil {
				catfileInfoChan = catfileInfoFilter(ctx, catfileInfoChan, tc.catfileInfoFilter)
			}

			catfileObjectChan := catfileObject(ctx, catfileProcess, catfileInfoChan)
			if tc.catfileObjectFilter != nil {
				catfileObjectChan = catfileObjectFilter(ctx, catfileObjectChan, tc.catfileObjectFilter)
			}

			var results []catfileObjectResult
			for result := range catfileObjectChan {
				// We're converting the error here to a plain un-nested error such
				// that we don't have to replicate the complete error's structure.
				if result.err != nil {
					result.err = errors.New(result.err.Error())
				}

				if result.err == nil {
					// While we could also assert object data, let's not do this: it
					// would just be too annoying.
					require.NotNil(t, result.objectData)
					require.Len(t, result.objectData, int(result.objectInfo.Size))
					result.objectData = nil
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

		revlistChan := revlist(childCtx, repo, []string{"--all"})
		revlistChan = revlistFilter(childCtx, revlistChan, func(revlistResult) bool { return true })
		catfileInfoChan := catfileInfo(childCtx, catfileProcess, revlistChan)
		catfileInfoChan = catfileInfoFilter(childCtx, catfileInfoChan, func(catfileInfoResult) bool { return true })
		catfileObjectChan := catfileObject(childCtx, catfileProcess, catfileInfoChan)
		catfileObjectChan = catfileObjectFilter(childCtx, catfileObjectChan, func(catfileObjectResult) bool { return true })

		i := 0
		for result := range catfileObjectChan {
			require.NoError(t, result.err)
			i++

			if i == 3 {
				cancel()
			}
		}

		// Context cancellation is timing sensitive: at the point of cancelling the context,
		// the last pipeline step may already have queued up an additional result. We thus
		// cannot assert the exact number of requests, but we know that it's bounded.
		require.LessOrEqual(t, i, 4)
	})
}
