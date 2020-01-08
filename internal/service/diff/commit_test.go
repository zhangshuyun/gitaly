package diff

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/diff"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCommitDiffRequest(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rightCommit := "ab2c9622c02288a2bbaaf35d96088cfdff31d9d9"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	expectedDiffs := []diff.Diff{
		{
			FromID:   "faaf198af3a36dbf41961466703cc1d47c61d051",
			ToID:     "877cee6ab11f9094e1bcdb7f1fd9c0001b572185",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("README.md"),
			ToPath:   []byte("README.md"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/readme-md-chunks.txt"),
		},
		{
			FromID:   "bdea48ee65c869eb0b86b1283069d76cce0a7254",
			ToID:     "0000000000000000000000000000000000000000",
			OldMode:  0100644,
			NewMode:  0,
			FromPath: []byte("gitaly/deleted-file"),
			ToPath:   []byte("gitaly/deleted-file"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/deleted-file-chunks.txt"),
		},
		{
			FromID:   "aa408b4556e594f7974390ad6b86210617fbda6e",
			ToID:     "1c69c4d2a65ad05c24ac3b6780b5748b97ffd3aa",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("gitaly/file-with-multiple-chunks"),
			ToPath:   []byte("gitaly/file-with-multiple-chunks"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/file-with-multiple-chunks-chunks.txt"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "389c7a36a6e133268b0d36b00e7ffc0f3a5b6651",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("gitaly/file-with-pluses.txt"),
			ToPath:   []byte("gitaly/file-with-pluses.txt"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/file-with-pluses-chunks.txt"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "bc2ef601a538d69ef99d5bdafa605e63f902e8e4",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("gitaly/logo-white.png"),
			ToPath:   []byte("gitaly/logo-white.png"),
			Binary:   true,
			Patch:    []byte("Binary files /dev/null and b/gitaly/logo-white.png differ\n"),
		},
		{
			FromID:   "ead5a0eee1391308803cfebd8a2a8530495645eb",
			ToID:     "ead5a0eee1391308803cfebd8a2a8530495645eb",
			OldMode:  0100644,
			NewMode:  0100755,
			FromPath: []byte("gitaly/mode-file"),
			ToPath:   []byte("gitaly/mode-file"),
			Binary:   false,
		},
		{
			FromID:   "357406f3075a57708d0163752905cc1576fceacc",
			ToID:     "8e5177d718c561d36efde08bad36b43687ee6bf0",
			OldMode:  0100644,
			NewMode:  0100755,
			FromPath: []byte("gitaly/mode-file-with-mods"),
			ToPath:   []byte("gitaly/mode-file-with-mods"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/mode-file-with-mods-chunks.txt"),
		},
		{
			FromID:   "43d24af4e22580f36b1ca52647c1aff75a766a33",
			ToID:     "0000000000000000000000000000000000000000",
			OldMode:  0100644,
			NewMode:  0,
			FromPath: []byte("gitaly/named-file-with-mods"),
			ToPath:   []byte("gitaly/named-file-with-mods"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/named-file-with-mods-chunks.txt"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "b464dff7a75ccc92fbd920fd9ae66a84b9d2bf94",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("gitaly/no-newline-at-the-end"),
			ToPath:   []byte("gitaly/no-newline-at-the-end"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/no-newline-at-the-end-chunks.txt"),
		},
		{
			FromID:   "4e76e90b3c7e52390de9311a23c0a77575aed8a8",
			ToID:     "4e76e90b3c7e52390de9311a23c0a77575aed8a8",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("gitaly/named-file"),
			ToPath:   []byte("gitaly/renamed-file"),
			Binary:   false,
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "3856c00e9450a51a62096327167fc43d3be62eef",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("gitaly/renamed-file-with-mods"),
			ToPath:   []byte("gitaly/renamed-file-with-mods"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/renamed-file-with-mods-chunks.txt"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "a135e3e0d4af177a902ca57dcc4c7fc6f30858b1",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("gitaly/tab\tnewline\n file"),
			ToPath:   []byte("gitaly/tab\tnewline\n file"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/tab-newline-file-chunks.txt"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
			OldMode:  0,
			NewMode:  0100755,
			FromPath: []byte("gitaly/テスト.txt"),
			ToPath:   []byte("gitaly/テスト.txt"),
			Binary:   false,
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "b1e67221afe8461efd244b487afca22d46b95eb8",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("z-short-diff"),
			ToPath:   []byte("z-short-diff"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/z-short-diff-chunks.txt"),
		},
	}

	testCases := []struct {
		noPrefixConfig string
		desc           string
	}{
		{noPrefixConfig: "false", desc: "Git config diff.noprefix set to false"},
		{noPrefixConfig: "true", desc: "Git config diff.noprefix set to true"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "diff.noprefix", testCase.noPrefixConfig)
			rpcRequest := &gitalypb.CommitDiffRequest{Repository: testRepo, RightCommitId: rightCommit, LeftCommitId: leftCommit, IgnoreWhitespaceChange: false}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitDiff(ctx, rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

			assertExactReceivedDiffs(t, c, expectedDiffs)
		})
	}
}

func TestSuccessfulCommitDiffRequestWithPaths(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rightCommit := "e4003da16c1c2c3fc4567700121b17bf8e591c6c"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.CommitDiffRequest{
		Repository:             testRepo,
		RightCommitId:          rightCommit,
		LeftCommitId:           leftCommit,
		IgnoreWhitespaceChange: false,
		Paths: [][]byte{
			[]byte("CONTRIBUTING.md"),
			[]byte("README.md"),
			[]byte("gitaly/named-file-with-mods"),
			[]byte("gitaly/mode-file-with-mods"),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.CommitDiff(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	expectedDiffs := []diff.Diff{
		{
			FromID:   "c1788657b95998a2f177a4f86d68a60f2a80117f",
			ToID:     "b87f61fe2d7b2e208b340a1f3cafea916bd27f75",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("CONTRIBUTING.md"),
			ToPath:   []byte("CONTRIBUTING.md"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/contributing-md-chunks.txt"),
		},
		{
			FromID:   "faaf198af3a36dbf41961466703cc1d47c61d051",
			ToID:     "877cee6ab11f9094e1bcdb7f1fd9c0001b572185",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("README.md"),
			ToPath:   []byte("README.md"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/readme-md-chunks.txt"),
		},
		{
			FromID:   "357406f3075a57708d0163752905cc1576fceacc",
			ToID:     "8e5177d718c561d36efde08bad36b43687ee6bf0",
			OldMode:  0100644,
			NewMode:  0100755,
			FromPath: []byte("gitaly/mode-file-with-mods"),
			ToPath:   []byte("gitaly/mode-file-with-mods"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/mode-file-with-mods-chunks.txt"),
		},
		{
			FromID:   "43d24af4e22580f36b1ca52647c1aff75a766a33",
			ToID:     "0000000000000000000000000000000000000000",
			OldMode:  0100644,
			NewMode:  0,
			FromPath: []byte("gitaly/named-file-with-mods"),
			ToPath:   []byte("gitaly/named-file-with-mods"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/named-file-with-mods-chunks.txt"),
		},
	}

	assertExactReceivedDiffs(t, c, expectedDiffs)
}

func TestSuccessfulCommitDiffRequestWithTypeChangeDiff(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rightCommit := "184a47d38677e2e439964859b877ae9bc424ab11"
	leftCommit := "80d56eb72ba5d77fd8af857eced17a7d0640cb82"
	rpcRequest := &gitalypb.CommitDiffRequest{
		Repository:    testRepo,
		RightCommitId: rightCommit,
		LeftCommitId:  leftCommit,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.CommitDiff(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	expectedDiffs := []diff.Diff{
		{
			FromID:   "349cd0f6b1aba8538861d95783cbce6d49d747f8",
			ToID:     "0000000000000000000000000000000000000000",
			OldMode:  0120000,
			NewMode:  0,
			FromPath: []byte("gitaly/symlink-to-be-regular"),
			ToPath:   []byte("gitaly/symlink-to-be-regular"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/symlink-to-be-regular-deleted-chunks.txt"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "f9e5cc857610185e6feeb494a26bf27551a4f02b",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("gitaly/symlink-to-be-regular"),
			ToPath:   []byte("gitaly/symlink-to-be-regular"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/symlink-to-be-regular-added-chunks.txt"),
		},
	}

	assertExactReceivedDiffs(t, c, expectedDiffs)
}

func TestSuccessfulCommitDiffRequestWithIgnoreWhitespaceChange(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rightCommit := "e4003da16c1c2c3fc4567700121b17bf8e591c6c"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"

	whitespacePaths := [][]byte{
		[]byte("CONTRIBUTING.md"),
		[]byte("MAINTENANCE.md"),
		[]byte("README.md"),
	}
	normalPaths := [][]byte{
		[]byte("gitaly/named-file-with-mods"),
		[]byte("gitaly/mode-file-with-mods"),
	}

	expectedWhitespaceDiffs := []diff.Diff{
		{
			FromID:   "c1788657b95998a2f177a4f86d68a60f2a80117f",
			ToID:     "b87f61fe2d7b2e208b340a1f3cafea916bd27f75",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("CONTRIBUTING.md"),
			ToPath:   []byte("CONTRIBUTING.md"),
			Binary:   false,
		},
		{
			FromID:   "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
			ToID:     "5d9c7c0470bf368d61d9b6cd076300dc9d061f14",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("MAINTENANCE.md"),
			ToPath:   []byte("MAINTENANCE.md"),
			Binary:   false,
		},
		{
			FromID:   "faaf198af3a36dbf41961466703cc1d47c61d051",
			ToID:     "877cee6ab11f9094e1bcdb7f1fd9c0001b572185",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("README.md"),
			ToPath:   []byte("README.md"),
			Binary:   false,
		},
	}
	expectedNormalDiffs := []diff.Diff{
		{
			FromID:   "357406f3075a57708d0163752905cc1576fceacc",
			ToID:     "8e5177d718c561d36efde08bad36b43687ee6bf0",
			OldMode:  0100644,
			NewMode:  0100755,
			FromPath: []byte("gitaly/mode-file-with-mods"),
			ToPath:   []byte("gitaly/mode-file-with-mods"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/mode-file-with-mods-chunks.txt"),
		},
		{
			FromID:   "43d24af4e22580f36b1ca52647c1aff75a766a33",
			ToID:     "0000000000000000000000000000000000000000",
			OldMode:  0100644,
			NewMode:  0,
			FromPath: []byte("gitaly/named-file-with-mods"),
			ToPath:   []byte("gitaly/named-file-with-mods"),
			Binary:   false,
			Patch:    testhelper.MustReadFile(t, "testdata/named-file-with-mods-chunks.txt"),
		},
	}

	pathsAndDiffs := []struct {
		desc  string
		paths [][]byte
		diffs []diff.Diff
	}{
		{
			desc:  "whitespace paths",
			paths: whitespacePaths,
			diffs: expectedWhitespaceDiffs,
		},
		{
			desc:  "whitespace paths and normal paths",
			paths: append(whitespacePaths, normalPaths...),
			diffs: append(expectedWhitespaceDiffs, expectedNormalDiffs...),
		},
	}

	for _, entry := range pathsAndDiffs {
		t.Run(entry.desc, func(t *testing.T) {
			rpcRequest := &gitalypb.CommitDiffRequest{
				Repository:             testRepo,
				RightCommitId:          rightCommit,
				LeftCommitId:           leftCommit,
				IgnoreWhitespaceChange: true,
				Paths:                  entry.paths,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitDiff(ctx, rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

			assertExactReceivedDiffs(t, c, entry.diffs)
		})
	}
}

func TestSuccessfulCommitDiffRequestWithLimits(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rightCommit := "899d3d27b04690ac1cd9ef4d8a74fde0667c57f1"
	leftCommit := "184a47d38677e2e439964859b877ae9bc424ab11"

	type diffAttributes struct {
		path                                string
		collapsed, overflowMarker, tooLarge bool
	}

	requestsAndResults := []struct {
		desc    string
		request gitalypb.CommitDiffRequest
		result  []diffAttributes
	}{
		{
			desc: "no enforcement",
			request: gitalypb.CommitDiffRequest{
				EnforceLimits: false,
			},
			result: []diffAttributes{
				{path: "CHANGELOG"},
				{path: "CONTRIBUTING.md"},
				{path: "LICENSE"},
				{path: "PROCESS.md"},
				{path: "VERSION"},
			},
		},
		{
			desc: "max file count enforcement",
			request: gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				MaxFiles:      3,
				MaxLines:      1000,
				MaxBytes:      3 * 5 * 1024,
				MaxPatchBytes: 100000,
			},
			result: []diffAttributes{
				{path: "CHANGELOG"},
				{path: "CONTRIBUTING.md"},
				{path: "LICENSE"},
				{overflowMarker: true},
			},
		},
		{
			desc: "max line count enforcement",
			request: gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				MaxFiles:      5,
				MaxLines:      90,
				MaxBytes:      5 * 5 * 1024,
				MaxPatchBytes: 100000,
			},
			result: []diffAttributes{
				{path: "CHANGELOG"},
				{path: "CONTRIBUTING.md"},
				{overflowMarker: true},
			},
		},
		{
			desc: "max byte count enforcement",
			request: gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				MaxFiles:      5,
				MaxLines:      1000,
				MaxBytes:      6900,
				MaxPatchBytes: 100000,
			},
			result: []diffAttributes{
				{path: "CHANGELOG"},
				{path: "CONTRIBUTING.md"},
				{path: "LICENSE"},
				{path: "PROCESS.md"},
				{overflowMarker: true},
			},
		},
		{
			desc: "no collapse",
			request: gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				CollapseDiffs: false,
				MaxFiles:      3,
				MaxLines:      1000,
				MaxBytes:      3 * 5 * 1024,
				SafeMaxFiles:  1,
				SafeMaxLines:  1000,
				SafeMaxBytes:  1 * 5 * 1024,
				MaxPatchBytes: 100000,
			},
			result: []diffAttributes{
				{path: "CHANGELOG"},
				{path: "CONTRIBUTING.md"},
				{path: "LICENSE"},
				{overflowMarker: true},
			},
		},
		{
			desc: "set as too large when exceeding single patch limit",
			request: gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				CollapseDiffs: false,
				MaxFiles:      5,
				MaxLines:      1000,
				MaxBytes:      3 * 5 * 1024,
				SafeMaxFiles:  3,
				SafeMaxLines:  1000,
				SafeMaxBytes:  1 * 5 * 1024,
				MaxPatchBytes: 1200,
			},
			result: []diffAttributes{
				{path: "CHANGELOG", tooLarge: true},
				{path: "CONTRIBUTING.md", tooLarge: true},
				{path: "LICENSE", tooLarge: false},
				{path: "PROCESS.md", tooLarge: true},
				{path: "VERSION", tooLarge: false},
			},
		},
		{
			desc: "collapse after safe max file count is exceeded",
			request: gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				CollapseDiffs: true,
				MaxFiles:      3,
				MaxLines:      1000,
				MaxBytes:      3 * 5 * 1024,
				SafeMaxFiles:  1,
				SafeMaxLines:  1000,
				SafeMaxBytes:  1 * 5 * 1024,
				MaxPatchBytes: 100000,
			},
			result: []diffAttributes{
				{path: "CHANGELOG"},
				{path: "CONTRIBUTING.md", collapsed: true},
				{path: "LICENSE", collapsed: true},
				{overflowMarker: true},
			},
		},
		{
			desc: "collapse after safe max line count is exceeded",
			request: gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				CollapseDiffs: true,
				MaxFiles:      5,
				MaxLines:      100,
				MaxBytes:      5 * 5 * 1024,
				SafeMaxFiles:  5,
				SafeMaxLines:  40,
				SafeMaxBytes:  5 * 5 * 1024,
				MaxPatchBytes: 100000,
			},
			result: []diffAttributes{
				{path: "CHANGELOG", collapsed: true},
				{path: "CONTRIBUTING.md", collapsed: true},
				{path: "LICENSE"},
				{path: "PROCESS.md", collapsed: true},
				{path: "VERSION"},
			},
		},
		{
			desc: "collapse after safe max byte count is exceeded",
			request: gitalypb.CommitDiffRequest{
				EnforceLimits: true,
				CollapseDiffs: true,
				MaxFiles:      4,
				MaxLines:      1000,
				MaxBytes:      4 * 5 * 1024,
				SafeMaxFiles:  4,
				SafeMaxLines:  1000,
				SafeMaxBytes:  4830,
				MaxPatchBytes: 100000,
			},
			result: []diffAttributes{
				{path: "CHANGELOG"},
				{path: "CONTRIBUTING.md"},
				{path: "LICENSE"},
				{path: "PROCESS.md", collapsed: true},
				{overflowMarker: true},
			},
		},
	}

	for _, requestAndResult := range requestsAndResults {
		t.Run(requestAndResult.desc, func(t *testing.T) {
			request := requestAndResult.request
			request.Repository = testRepo
			request.LeftCommitId = leftCommit
			request.RightCommitId = rightCommit

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitDiff(ctx, &request)
			if err != nil {
				t.Fatal(err)
			}

			receivedDiffs := getDiffsFromCommitDiffClient(t, c)

			require.Equal(t, len(requestAndResult.result), len(receivedDiffs), "number of diffs received")
			for i, diff := range receivedDiffs {
				expectedDiff := requestAndResult.result[i]

				require.Equal(t, expectedDiff.overflowMarker, diff.OverflowMarker, "%s overflow marker", diff.FromPath)
				require.Equal(t, expectedDiff.tooLarge, diff.TooLarge, "%s too large", diff.FromPath)
				require.Equal(t, expectedDiff.path, string(diff.FromPath), "%s path", diff.FromPath)
				require.Equal(t, expectedDiff.collapsed, diff.Collapsed, "%s collapsed", diff.FromPath)

				if expectedDiff.collapsed {
					require.Empty(t, diff.Patch, "patch")
				}
			}
		})
	}
}

func TestFailedCommitDiffRequestDueToValidationError(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rightCommit := "d42783470dc29fde2cf459eb3199ee1d7e3f3a72"
	leftCommit := rightCommit + "~" // Parent of rightCommit

	rpcRequests := []gitalypb.CommitDiffRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, RightCommitId: rightCommit, LeftCommitId: leftCommit}, // Repository doesn't exist
		{Repository: nil, RightCommitId: rightCommit, LeftCommitId: leftCommit},                                                             // Repository is nil
		{Repository: testRepo, RightCommitId: "", LeftCommitId: leftCommit},                                                                 // RightCommitId is empty
		{Repository: testRepo, RightCommitId: rightCommit, LeftCommitId: ""},                                                                // LeftCommitId is empty
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitDiff(ctx, &rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

			err = drainCommitDiffResponse(c)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestFailedCommitDiffRequestWithNonExistentCommit(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	nonExistentCommitID := "deadfacedeadfacedeadfacedeadfacedeadface"
	leftCommit := nonExistentCommitID + "~" // Parent of rightCommit
	rpcRequest := &gitalypb.CommitDiffRequest{Repository: testRepo, RightCommitId: nonExistentCommitID, LeftCommitId: leftCommit}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.CommitDiff(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	err = drainCommitDiffResponse(c)
	testhelper.RequireGrpcError(t, err, codes.Unavailable)
}

func TestSuccessfulCommitDeltaRequest(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rightCommit := "742518b2be68fc750bb4c357c0df821a88113286"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.CommitDeltaRequest{Repository: testRepo, RightCommitId: rightCommit, LeftCommitId: leftCommit}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.CommitDelta(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	expectedDeltas := []diff.Diff{
		{
			FromID:   "faaf198af3a36dbf41961466703cc1d47c61d051",
			ToID:     "877cee6ab11f9094e1bcdb7f1fd9c0001b572185",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("README.md"),
			ToPath:   []byte("README.md"),
		},
		{
			FromID:   "bdea48ee65c869eb0b86b1283069d76cce0a7254",
			ToID:     "0000000000000000000000000000000000000000",
			OldMode:  0100644,
			NewMode:  0,
			FromPath: []byte("gitaly/deleted-file"),
			ToPath:   []byte("gitaly/deleted-file"),
		},
		{
			FromID:   "aa408b4556e594f7974390ad6b86210617fbda6e",
			ToID:     "1c69c4d2a65ad05c24ac3b6780b5748b97ffd3aa",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("gitaly/file-with-multiple-chunks"),
			ToPath:   []byte("gitaly/file-with-multiple-chunks"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "bc2ef601a538d69ef99d5bdafa605e63f902e8e4",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("gitaly/logo-white.png"),
			ToPath:   []byte("gitaly/logo-white.png"),
		},
		{
			FromID:   "ead5a0eee1391308803cfebd8a2a8530495645eb",
			ToID:     "ead5a0eee1391308803cfebd8a2a8530495645eb",
			OldMode:  0100644,
			NewMode:  0100755,
			FromPath: []byte("gitaly/mode-file"),
			ToPath:   []byte("gitaly/mode-file"),
		},
		{
			FromID:   "357406f3075a57708d0163752905cc1576fceacc",
			ToID:     "8e5177d718c561d36efde08bad36b43687ee6bf0",
			OldMode:  0100644,
			NewMode:  0100755,
			FromPath: []byte("gitaly/mode-file-with-mods"),
			ToPath:   []byte("gitaly/mode-file-with-mods"),
		},
		{
			FromID:   "43d24af4e22580f36b1ca52647c1aff75a766a33",
			ToID:     "0000000000000000000000000000000000000000",
			OldMode:  0100644,
			NewMode:  0,
			FromPath: []byte("gitaly/named-file-with-mods"),
			ToPath:   []byte("gitaly/named-file-with-mods"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "b464dff7a75ccc92fbd920fd9ae66a84b9d2bf94",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("gitaly/no-newline-at-the-end"),
			ToPath:   []byte("gitaly/no-newline-at-the-end"),
		},
		{
			FromID:   "4e76e90b3c7e52390de9311a23c0a77575aed8a8",
			ToID:     "4e76e90b3c7e52390de9311a23c0a77575aed8a8",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("gitaly/named-file"),
			ToPath:   []byte("gitaly/renamed-file"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "3856c00e9450a51a62096327167fc43d3be62eef",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("gitaly/renamed-file-with-mods"),
			ToPath:   []byte("gitaly/renamed-file-with-mods"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "a135e3e0d4af177a902ca57dcc4c7fc6f30858b1",
			OldMode:  0,
			NewMode:  0100644,
			FromPath: []byte("gitaly/tab\tnewline\n file"),
			ToPath:   []byte("gitaly/tab\tnewline\n file"),
		},
		{
			FromID:   "0000000000000000000000000000000000000000",
			ToID:     "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
			OldMode:  0,
			NewMode:  0100755,
			FromPath: []byte("gitaly/テスト.txt"),
			ToPath:   []byte("gitaly/テスト.txt"),
		},
	}

	assertExactReceivedDeltas(t, c, expectedDeltas)
}

func TestSuccessfulCommitDeltaRequestWithPaths(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rightCommit := "e4003da16c1c2c3fc4567700121b17bf8e591c6c"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.CommitDeltaRequest{
		Repository:    testRepo,
		RightCommitId: rightCommit,
		LeftCommitId:  leftCommit,
		Paths: [][]byte{
			[]byte("CONTRIBUTING.md"),
			[]byte("README.md"),
			[]byte("gitaly/named-file-with-mods"),
			[]byte("gitaly/mode-file-with-mods"),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.CommitDelta(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	expectedDeltas := []diff.Diff{
		{
			FromID:   "c1788657b95998a2f177a4f86d68a60f2a80117f",
			ToID:     "b87f61fe2d7b2e208b340a1f3cafea916bd27f75",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("CONTRIBUTING.md"),
			ToPath:   []byte("CONTRIBUTING.md"),
		},
		{
			FromID:   "faaf198af3a36dbf41961466703cc1d47c61d051",
			ToID:     "877cee6ab11f9094e1bcdb7f1fd9c0001b572185",
			OldMode:  0100644,
			NewMode:  0100644,
			FromPath: []byte("README.md"),
			ToPath:   []byte("README.md"),
		},
		{
			FromID:   "357406f3075a57708d0163752905cc1576fceacc",
			ToID:     "8e5177d718c561d36efde08bad36b43687ee6bf0",
			OldMode:  0100644,
			NewMode:  0100755,
			FromPath: []byte("gitaly/mode-file-with-mods"),
			ToPath:   []byte("gitaly/mode-file-with-mods"),
		},
		{
			FromID:   "43d24af4e22580f36b1ca52647c1aff75a766a33",
			ToID:     "0000000000000000000000000000000000000000",
			OldMode:  0100644,
			NewMode:  0,
			FromPath: []byte("gitaly/named-file-with-mods"),
			ToPath:   []byte("gitaly/named-file-with-mods"),
		},
	}

	assertExactReceivedDeltas(t, c, expectedDeltas)
}

func TestFailedCommitDeltaRequestDueToValidationError(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rightCommit := "d42783470dc29fde2cf459eb3199ee1d7e3f3a72"
	leftCommit := rightCommit + "~" // Parent of rightCommit

	rpcRequests := []gitalypb.CommitDeltaRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, RightCommitId: rightCommit, LeftCommitId: leftCommit}, // Repository doesn't exist
		{Repository: nil, RightCommitId: rightCommit, LeftCommitId: leftCommit},                                                             // Repository is nil
		{Repository: testRepo, RightCommitId: "", LeftCommitId: leftCommit},                                                                 // RightCommitId is empty
		{Repository: testRepo, RightCommitId: rightCommit, LeftCommitId: ""},                                                                // LeftCommitId is empty
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitDelta(ctx, &rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

			err = drainCommitDeltaResponse(c)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestFailedCommitDeltaRequestWithNonExistentCommit(t *testing.T) {
	stop, serverSocketPath := runDiffServer(t)
	defer stop()

	client, conn := newDiffClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	nonExistentCommitID := "deadfacedeadfacedeadfacedeadfacedeadface"
	leftCommit := nonExistentCommitID + "~" // Parent of rightCommit
	rpcRequest := &gitalypb.CommitDeltaRequest{Repository: testRepo, RightCommitId: nonExistentCommitID, LeftCommitId: leftCommit}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.CommitDelta(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	err = drainCommitDeltaResponse(c)
	testhelper.RequireGrpcError(t, err, codes.Unavailable)
}

func drainCommitDiffResponse(c gitalypb.DiffService_CommitDiffClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}
}

func drainCommitDeltaResponse(c gitalypb.DiffService_CommitDeltaClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}
}

func getDiffsFromCommitDiffClient(t *testing.T, client gitalypb.DiffService_CommitDiffClient) []*diff.Diff {
	var diffs []*diff.Diff
	var currentDiff *diff.Diff

	for {
		fetchedDiff, err := client.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		if currentDiff == nil {
			currentDiff = &diff.Diff{
				FromID:         fetchedDiff.FromId,
				ToID:           fetchedDiff.ToId,
				OldMode:        fetchedDiff.OldMode,
				NewMode:        fetchedDiff.NewMode,
				FromPath:       fetchedDiff.FromPath,
				ToPath:         fetchedDiff.ToPath,
				Binary:         fetchedDiff.Binary,
				Collapsed:      fetchedDiff.Collapsed,
				OverflowMarker: fetchedDiff.OverflowMarker,
				Patch:          fetchedDiff.RawPatchData,
				TooLarge:       fetchedDiff.TooLarge,
			}
		} else {
			currentDiff.Patch = append(currentDiff.Patch, fetchedDiff.RawPatchData...)
		}

		if fetchedDiff.EndOfPatch {
			diffs = append(diffs, currentDiff)
			currentDiff = nil
		}
	}

	return diffs
}

func assertExactReceivedDiffs(t *testing.T, client gitalypb.DiffService_CommitDiffClient, expectedDiffs []diff.Diff) {
	fetchedDiffs := getDiffsFromCommitDiffClient(t, client)

	var i int
	var fetchedDiff *diff.Diff

	for i, fetchedDiff = range fetchedDiffs {
		if i >= len(expectedDiffs) {
			t.Errorf("Unexpected diff #%d received: %v", i, fetchedDiff)
			break
		}

		expectedDiff := expectedDiffs[i]

		if expectedDiff.FromID != fetchedDiff.FromID {
			t.Errorf("Expected diff #%d FromID to equal = %q, got %q", i, expectedDiff.FromID, fetchedDiff.FromID)
		}

		if expectedDiff.ToID != fetchedDiff.ToID {
			t.Errorf("Expected diff #%d ToID to equal = %q, got %q", i, expectedDiff.ToID, fetchedDiff.ToID)
		}

		if expectedDiff.OldMode != fetchedDiff.OldMode {
			t.Errorf("Expected diff #%d OldMode to equal = %o, got %o", i, expectedDiff.OldMode, fetchedDiff.OldMode)
		}

		if expectedDiff.NewMode != fetchedDiff.NewMode {
			t.Errorf("Expected diff #%d NewMode to equal = %o, got %o", i, expectedDiff.NewMode, fetchedDiff.NewMode)
		}

		if !bytes.Equal(expectedDiff.FromPath, fetchedDiff.FromPath) {
			t.Errorf("Expected diff #%d FromPath to equal = %q, got %q", i, expectedDiff.FromPath, fetchedDiff.FromPath)
		}

		if !bytes.Equal(expectedDiff.ToPath, fetchedDiff.ToPath) {
			t.Errorf("Expected diff #%d ToPath to equal = %q, got %q", i, expectedDiff.ToPath, fetchedDiff.ToPath)
		}

		if expectedDiff.Binary != fetchedDiff.Binary {
			t.Errorf("Expected diff #%d Binary to be %t, got %t", i, expectedDiff.Binary, fetchedDiff.Binary)
		}

		if !bytes.Equal(expectedDiff.Patch, fetchedDiff.Patch) {
			t.Errorf("Expected diff #%d Chunks to be %q, got %q", i, expectedDiff.Patch, fetchedDiff.Patch)
		}
	}

	if len(expectedDiffs) != i+1 {
		t.Errorf("Expected number of diffs to be %d, got %d", len(expectedDiffs), i+1)
	}
}

func assertExactReceivedDeltas(t *testing.T, client gitalypb.DiffService_CommitDeltaClient, expectedDeltas []diff.Diff) {
	i := 0
	for {
		fetchedDeltas, err := client.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		for _, fetchedDelta := range fetchedDeltas.GetDeltas() {
			if i >= len(expectedDeltas) {
				t.Errorf("Unexpected delta #%d received: %v", i, fetchedDelta)
				break
			}

			expectedDelta := expectedDeltas[i]

			if expectedDelta.FromID != fetchedDelta.FromId {
				t.Errorf("Expected delta #%d FromID to equal = %q, got %q", i, expectedDelta.FromID, fetchedDelta.FromId)
			}

			if expectedDelta.ToID != fetchedDelta.ToId {
				t.Errorf("Expected delta #%d ToID to equal = %q, got %q", i, expectedDelta.ToID, fetchedDelta.ToId)
			}

			if expectedDelta.OldMode != fetchedDelta.OldMode {
				t.Errorf("Expected delta #%d OldMode to equal = %o, got %o", i, expectedDelta.OldMode, fetchedDelta.OldMode)
			}

			if expectedDelta.NewMode != fetchedDelta.NewMode {
				t.Errorf("Expected delta #%d NewMode to equal = %o, got %o", i, expectedDelta.NewMode, fetchedDelta.NewMode)
			}

			if !bytes.Equal(expectedDelta.FromPath, fetchedDelta.FromPath) {
				t.Errorf("Expected delta #%d FromPath to equal = %q, got %q", i, expectedDelta.FromPath, fetchedDelta.FromPath)
			}

			if !bytes.Equal(expectedDelta.ToPath, fetchedDelta.ToPath) {
				t.Errorf("Expected delta #%d ToPath to equal = %q, got %q", i, expectedDelta.ToPath, fetchedDelta.ToPath)
			}

			i++
		}
	}

	if len(expectedDeltas) != i {
		t.Errorf("Expected number of deltas to be %d, got %d", len(expectedDeltas), i)
	}
}
