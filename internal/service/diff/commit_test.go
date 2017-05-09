package diff

import (
	"bytes"
	"io"
	"net"
	"path"
	"testing"
	"time"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/diff"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
)

var serverSocketPath = path.Join(scratchDir, "gitaly.sock")

type expectedDiff struct {
	diff.Diff
	ChunksCombined []byte
}

func TestSuccessfulCommitDiffRequest(t *testing.T) {
	server := runDiffServer(t)
	defer server.Stop()

	client := newDiffClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	rightCommit := "742518b2be68fc750bb4c357c0df821a88113286"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &pb.CommitDiffRequest{Repository: repo, RightCommitId: rightCommit, LeftCommitId: leftCommit, IgnoreWhitespaceChange: false}

	c, err := client.CommitDiff(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	expectedDiffs := []expectedDiff{
		{
			Diff: diff.Diff{
				FromID:   "faaf198af3a36dbf41961466703cc1d47c61d051",
				ToID:     "877cee6ab11f9094e1bcdb7f1fd9c0001b572185",
				OldMode:  0100644,
				NewMode:  0100644,
				FromPath: []byte("README.md"),
				ToPath:   []byte("README.md"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/readme-md-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "bdea48ee65c869eb0b86b1283069d76cce0a7254",
				ToID:     "0000000000000000000000000000000000000000",
				OldMode:  0100644,
				NewMode:  0,
				FromPath: []byte("gitaly/deleted-file"),
				ToPath:   []byte("gitaly/deleted-file"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/deleted-file-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "aa408b4556e594f7974390ad6b86210617fbda6e",
				ToID:     "1c69c4d2a65ad05c24ac3b6780b5748b97ffd3aa",
				OldMode:  0100644,
				NewMode:  0100644,
				FromPath: []byte("gitaly/file-with-multiple-chunks"),
				ToPath:   []byte("gitaly/file-with-multiple-chunks"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/file-with-multiple-chunks-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "0000000000000000000000000000000000000000",
				ToID:     "bc2ef601a538d69ef99d5bdafa605e63f902e8e4",
				OldMode:  0,
				NewMode:  0100644,
				FromPath: []byte("gitaly/logo-white.png"),
				ToPath:   []byte("gitaly/logo-white.png"),
				Binary:   true,
			},
		},
		{
			Diff: diff.Diff{
				FromID:   "ead5a0eee1391308803cfebd8a2a8530495645eb",
				ToID:     "ead5a0eee1391308803cfebd8a2a8530495645eb",
				OldMode:  0100644,
				NewMode:  0100755,
				FromPath: []byte("gitaly/mode-file"),
				ToPath:   []byte("gitaly/mode-file"),
				Binary:   false,
			},
		},
		{
			Diff: diff.Diff{
				FromID:   "357406f3075a57708d0163752905cc1576fceacc",
				ToID:     "8e5177d718c561d36efde08bad36b43687ee6bf0",
				OldMode:  0100644,
				NewMode:  0100755,
				FromPath: []byte("gitaly/mode-file-with-mods"),
				ToPath:   []byte("gitaly/mode-file-with-mods"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/mode-file-with-mods-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "43d24af4e22580f36b1ca52647c1aff75a766a33",
				ToID:     "0000000000000000000000000000000000000000",
				OldMode:  0100644,
				NewMode:  0,
				FromPath: []byte("gitaly/named-file-with-mods"),
				ToPath:   []byte("gitaly/named-file-with-mods"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/named-file-with-mods-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "0000000000000000000000000000000000000000",
				ToID:     "b464dff7a75ccc92fbd920fd9ae66a84b9d2bf94",
				OldMode:  0,
				NewMode:  0100644,
				FromPath: []byte("gitaly/no-newline-at-the-end"),
				ToPath:   []byte("gitaly/no-newline-at-the-end"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/no-newline-at-the-end-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "4e76e90b3c7e52390de9311a23c0a77575aed8a8",
				ToID:     "4e76e90b3c7e52390de9311a23c0a77575aed8a8",
				OldMode:  0100644,
				NewMode:  0100644,
				FromPath: []byte("gitaly/named-file"),
				ToPath:   []byte("gitaly/renamed-file"),
				Binary:   false,
			},
		},
		{
			Diff: diff.Diff{
				FromID:   "0000000000000000000000000000000000000000",
				ToID:     "3856c00e9450a51a62096327167fc43d3be62eef",
				OldMode:  0,
				NewMode:  0100644,
				FromPath: []byte("gitaly/renamed-file-with-mods"),
				ToPath:   []byte("gitaly/renamed-file-with-mods"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/renamed-file-with-mods-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "0000000000000000000000000000000000000000",
				ToID:     "a135e3e0d4af177a902ca57dcc4c7fc6f30858b1",
				OldMode:  0,
				NewMode:  0100644,
				FromPath: []byte("gitaly/tab\tnewline\n file"),
				ToPath:   []byte("gitaly/tab\tnewline\n file"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/tab-newline-file-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "0000000000000000000000000000000000000000",
				ToID:     "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
				OldMode:  0,
				NewMode:  0100755,
				FromPath: []byte("gitaly/テスト.txt"),
				ToPath:   []byte("gitaly/テスト.txt"),
				Binary:   false,
			},
		},
	}

	assertExactReceivedDiffs(t, c, expectedDiffs)
}

func TestSuccessfulCommitDiffRequestWithPaths(t *testing.T) {
	server := runDiffServer(t)
	defer server.Stop()

	client := newDiffClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	rightCommit := "e4003da16c1c2c3fc4567700121b17bf8e591c6c"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &pb.CommitDiffRequest{
		Repository:             repo,
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

	c, err := client.CommitDiff(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	expectedDiffs := []expectedDiff{
		{
			Diff: diff.Diff{
				FromID:   "c1788657b95998a2f177a4f86d68a60f2a80117f",
				ToID:     "b87f61fe2d7b2e208b340a1f3cafea916bd27f75",
				OldMode:  0100644,
				NewMode:  0100644,
				FromPath: []byte("CONTRIBUTING.md"),
				ToPath:   []byte("CONTRIBUTING.md"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/contributing-md-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "faaf198af3a36dbf41961466703cc1d47c61d051",
				ToID:     "877cee6ab11f9094e1bcdb7f1fd9c0001b572185",
				OldMode:  0100644,
				NewMode:  0100644,
				FromPath: []byte("README.md"),
				ToPath:   []byte("README.md"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/readme-md-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "357406f3075a57708d0163752905cc1576fceacc",
				ToID:     "8e5177d718c561d36efde08bad36b43687ee6bf0",
				OldMode:  0100644,
				NewMode:  0100755,
				FromPath: []byte("gitaly/mode-file-with-mods"),
				ToPath:   []byte("gitaly/mode-file-with-mods"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/mode-file-with-mods-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "43d24af4e22580f36b1ca52647c1aff75a766a33",
				ToID:     "0000000000000000000000000000000000000000",
				OldMode:  0100644,
				NewMode:  0,
				FromPath: []byte("gitaly/named-file-with-mods"),
				ToPath:   []byte("gitaly/named-file-with-mods"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/named-file-with-mods-chunks.txt"),
		},
	}

	assertExactReceivedDiffs(t, c, expectedDiffs)
}

func TestSuccessfulCommitDiffRequestWithTypeChangeDiff(t *testing.T) {
	server := runDiffServer(t)
	defer server.Stop()

	client := newDiffClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	rightCommit := "184a47d38677e2e439964859b877ae9bc424ab11"
	leftCommit := "80d56eb72ba5d77fd8af857eced17a7d0640cb82"
	rpcRequest := &pb.CommitDiffRequest{
		Repository:    repo,
		RightCommitId: rightCommit,
		LeftCommitId:  leftCommit,
	}

	c, err := client.CommitDiff(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	expectedDiffs := []expectedDiff{
		{
			Diff: diff.Diff{
				FromID:   "349cd0f6b1aba8538861d95783cbce6d49d747f8",
				ToID:     "0000000000000000000000000000000000000000",
				OldMode:  0120000,
				NewMode:  0,
				FromPath: []byte("gitaly/symlink-to-be-regular"),
				ToPath:   []byte("gitaly/symlink-to-be-regular"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/symlink-to-be-regular-deleted-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "0000000000000000000000000000000000000000",
				ToID:     "f9e5cc857610185e6feeb494a26bf27551a4f02b",
				OldMode:  0,
				NewMode:  0100644,
				FromPath: []byte("gitaly/symlink-to-be-regular"),
				ToPath:   []byte("gitaly/symlink-to-be-regular"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/symlink-to-be-regular-added-chunks.txt"),
		},
	}

	assertExactReceivedDiffs(t, c, expectedDiffs)
}

func TestSuccessfulCommitDiffRequestWithIgnoreWhitespaceChange(t *testing.T) {
	server := runDiffServer(t)
	defer server.Stop()

	client := newDiffClient(t)
	repo := &pb.Repository{Path: testRepoPath}
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

	expectedWhitespaceDiffs := []expectedDiff{
		{
			Diff: diff.Diff{
				FromID:   "c1788657b95998a2f177a4f86d68a60f2a80117f",
				ToID:     "b87f61fe2d7b2e208b340a1f3cafea916bd27f75",
				OldMode:  0100644,
				NewMode:  0100644,
				FromPath: []byte("CONTRIBUTING.md"),
				ToPath:   []byte("CONTRIBUTING.md"),
				Binary:   false,
			},
		},
		{
			Diff: diff.Diff{
				FromID:   "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
				ToID:     "5d9c7c0470bf368d61d9b6cd076300dc9d061f14",
				OldMode:  0100644,
				NewMode:  0100644,
				FromPath: []byte("MAINTENANCE.md"),
				ToPath:   []byte("MAINTENANCE.md"),
				Binary:   false,
			},
		},
		{
			Diff: diff.Diff{
				FromID:   "faaf198af3a36dbf41961466703cc1d47c61d051",
				ToID:     "877cee6ab11f9094e1bcdb7f1fd9c0001b572185",
				OldMode:  0100644,
				NewMode:  0100644,
				FromPath: []byte("README.md"),
				ToPath:   []byte("README.md"),
				Binary:   false,
			},
		},
	}
	expectedNormalDiffs := []expectedDiff{
		{
			Diff: diff.Diff{
				FromID:   "357406f3075a57708d0163752905cc1576fceacc",
				ToID:     "8e5177d718c561d36efde08bad36b43687ee6bf0",
				OldMode:  0100644,
				NewMode:  0100755,
				FromPath: []byte("gitaly/mode-file-with-mods"),
				ToPath:   []byte("gitaly/mode-file-with-mods"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/mode-file-with-mods-chunks.txt"),
		},
		{
			Diff: diff.Diff{
				FromID:   "43d24af4e22580f36b1ca52647c1aff75a766a33",
				ToID:     "0000000000000000000000000000000000000000",
				OldMode:  0100644,
				NewMode:  0,
				FromPath: []byte("gitaly/named-file-with-mods"),
				ToPath:   []byte("gitaly/named-file-with-mods"),
				Binary:   false,
			},
			ChunksCombined: testhelper.MustReadFile(t, "testdata/named-file-with-mods-chunks.txt"),
		},
	}

	pathsAndDiffs := []struct {
		paths [][]byte
		diffs []expectedDiff
	}{
		{
			paths: whitespacePaths,
			diffs: expectedWhitespaceDiffs,
		},
		{
			paths: append(whitespacePaths, normalPaths...),
			diffs: append(expectedWhitespaceDiffs, expectedNormalDiffs...),
		},
	}

	for _, entry := range pathsAndDiffs {
		rpcRequest := &pb.CommitDiffRequest{
			Repository:             repo,
			RightCommitId:          rightCommit,
			LeftCommitId:           leftCommit,
			IgnoreWhitespaceChange: true,
			Paths: entry.paths,
		}

		c, err := client.CommitDiff(context.Background(), rpcRequest)
		if err != nil {
			t.Fatal(err)
		}

		assertExactReceivedDiffs(t, c, entry.diffs)
	}
}

func TestFailedCommitDiffRequestDueToValidationError(t *testing.T) {
	server := runDiffServer(t)
	defer server.Stop()

	client := newDiffClient(t)
	rightCommit := "d42783470dc29fde2cf459eb3199ee1d7e3f3a72"
	leftCommit := rightCommit + "~" // Parent of rightCommit

	rpcRequests := []pb.CommitDiffRequest{
		{Repository: &pb.Repository{Path: ""}, RightCommitId: rightCommit, LeftCommitId: leftCommit},   // Repository.Path is empty
		{Repository: nil, RightCommitId: rightCommit, LeftCommitId: leftCommit},                        // Repository is nil
		{Repository: &pb.Repository{Path: testRepoPath}, RightCommitId: "", LeftCommitId: leftCommit},  // RightCommitId is empty
		{Repository: &pb.Repository{Path: testRepoPath}, RightCommitId: rightCommit, LeftCommitId: ""}, // LeftCommitId is empty
	}

	for _, rpcRequest := range rpcRequests {
		t.Logf("test case: %v", rpcRequest)

		c, err := client.CommitDiff(context.Background(), &rpcRequest)
		if err != nil {
			t.Fatal(err)
		}

		err = drainCommitDiffResponse(c)
		testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
	}
}

func TestFailedCommitDiffRequestWithNonExistentCommit(t *testing.T) {
	server := runDiffServer(t)
	defer server.Stop()

	client := newDiffClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	nonExistentCommitID := "deadfacedeadfacedeadfacedeadfacedeadface"
	leftCommit := nonExistentCommitID + "~" // Parent of rightCommit
	rpcRequest := &pb.CommitDiffRequest{Repository: repo, RightCommitId: nonExistentCommitID, LeftCommitId: leftCommit}

	c, err := client.CommitDiff(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	err = drainCommitDiffResponse(c)
	testhelper.AssertGrpcError(t, err, codes.Unavailable, "")
}

func TestSuccessfulCommitDeltaRequest(t *testing.T) {
	server := runDiffServer(t)
	defer server.Stop()

	client := newDiffClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	rightCommit := "742518b2be68fc750bb4c357c0df821a88113286"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &pb.CommitDeltaRequest{Repository: repo, RightCommitId: rightCommit, LeftCommitId: leftCommit}

	c, err := client.CommitDelta(context.Background(), rpcRequest)
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
	server := runDiffServer(t)
	defer server.Stop()

	client := newDiffClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	rightCommit := "e4003da16c1c2c3fc4567700121b17bf8e591c6c"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &pb.CommitDeltaRequest{
		Repository:    repo,
		RightCommitId: rightCommit,
		LeftCommitId:  leftCommit,
		Paths: [][]byte{
			[]byte("CONTRIBUTING.md"),
			[]byte("README.md"),
			[]byte("gitaly/named-file-with-mods"),
			[]byte("gitaly/mode-file-with-mods"),
		},
	}

	c, err := client.CommitDelta(context.Background(), rpcRequest)
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
	server := runDiffServer(t)
	defer server.Stop()

	client := newDiffClient(t)
	rightCommit := "d42783470dc29fde2cf459eb3199ee1d7e3f3a72"
	leftCommit := rightCommit + "~" // Parent of rightCommit

	rpcRequests := []pb.CommitDeltaRequest{
		{Repository: &pb.Repository{Path: ""}, RightCommitId: rightCommit, LeftCommitId: leftCommit},   // Repository.Path is empty
		{Repository: nil, RightCommitId: rightCommit, LeftCommitId: leftCommit},                        // Repository is nil
		{Repository: &pb.Repository{Path: testRepoPath}, RightCommitId: "", LeftCommitId: leftCommit},  // RightCommitId is empty
		{Repository: &pb.Repository{Path: testRepoPath}, RightCommitId: rightCommit, LeftCommitId: ""}, // LeftCommitId is empty
	}

	for _, rpcRequest := range rpcRequests {
		t.Logf("test case: %v", rpcRequest)

		c, err := client.CommitDelta(context.Background(), &rpcRequest)
		if err != nil {
			t.Fatal(err)
		}

		err = drainCommitDeltaResponse(c)
		testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
	}
}

func TestFailedCommitDeltaRequestWithNonExistentCommit(t *testing.T) {
	server := runDiffServer(t)
	defer server.Stop()

	client := newDiffClient(t)
	repo := &pb.Repository{Path: testRepoPath}
	nonExistentCommitID := "deadfacedeadfacedeadfacedeadfacedeadface"
	leftCommit := nonExistentCommitID + "~" // Parent of rightCommit
	rpcRequest := &pb.CommitDeltaRequest{Repository: repo, RightCommitId: nonExistentCommitID, LeftCommitId: leftCommit}

	c, err := client.CommitDelta(context.Background(), rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	err = drainCommitDeltaResponse(c)
	testhelper.AssertGrpcError(t, err, codes.Unavailable, "")
}

func runDiffServer(t *testing.T) *grpc.Server {
	server := grpc.NewServer()
	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	pb.RegisterDiffServer(server, NewServer())
	reflection.Register(server)

	go server.Serve(listener)

	return server
}

func newDiffClient(t *testing.T) pb.DiffClient {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, _ time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return pb.NewDiffClient(conn)
}

func drainCommitDiffResponse(c pb.Diff_CommitDiffClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}

	return nil
}

func drainCommitDeltaResponse(c pb.Diff_CommitDeltaClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}

	return nil
}

func assertExactReceivedDiffs(t *testing.T, client pb.Diff_CommitDiffClient, expectedDiffs []expectedDiff) {
	i := 0
	for {
		fetchedDiff, err := client.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		if i >= len(expectedDiffs) {
			t.Errorf("Unexpected diff #%d received: %v", i, fetchedDiff)
			break
		}

		expectedDiff := expectedDiffs[i]

		if expectedDiff.FromID != fetchedDiff.FromId {
			t.Errorf("Expected diff #%d FromID to equal = %q, got %q", i, expectedDiff.FromID, fetchedDiff.FromId)
		}

		if expectedDiff.ToID != fetchedDiff.ToId {
			t.Errorf("Expected diff #%d ToID to equal = %q, got %q", i, expectedDiff.ToID, fetchedDiff.ToId)
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

		fetchedChunksCombined := bytes.Join(fetchedDiff.RawChunks, nil)
		if !bytes.Equal(expectedDiff.ChunksCombined, fetchedChunksCombined) {
			t.Errorf("Expected diff #%d Chunks to be %q, got %q", i, expectedDiff.ChunksCombined, fetchedChunksCombined)
		}

		i++
	}

	if len(expectedDiffs) != i {
		t.Errorf("Expected number of diffs to be %d, got %d", len(expectedDiffs), i)
	}
}

func assertExactReceivedDeltas(t *testing.T, client pb.Diff_CommitDeltaClient, expectedDeltas []diff.Diff) {
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
