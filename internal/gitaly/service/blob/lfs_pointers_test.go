package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	lfsPointer1 = "0c304a93cb8430108629bbbcaa27db3343299bc0"
	lfsPointer2 = "f78df813119a79bfbe0442ab92540a61d3ab7ff3"
	lfsPointer3 = "bab31d249f78fba464d1b75799aad496cc07fa3b"
	lfsPointer4 = "125fcc9f6e33175cb278b9b2809154d2535fe19f"
	lfsPointer5 = "0360724a0d64498331888f1eaef2d24243809230"
	lfsPointer6 = "ff0ab3afd1616ff78d0331865d922df103b64cf0"
)

var (
	lfsPointers = map[string]*gitalypb.LFSPointer{
		lfsPointer1: &gitalypb.LFSPointer{
			Size: 133,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:91eff75a492a3ed0dfcb544d7f31326bc4014c8551849c192fd1e48d4dd2c897\nsize 1575078\n\n"),
			Oid:  lfsPointer1,
		},
		lfsPointer2: &gitalypb.LFSPointer{
			Size: 127,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:f2b0a1e7550e9b718dafc9b525a04879a766de62e4fbdfc46593d47f7ab74636\nsize 20\n"),
			Oid:  lfsPointer2,
		},
		lfsPointer3: &gitalypb.LFSPointer{
			Size: 127,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:bad71f905b60729f502ca339f7c9f001281a3d12c68a5da7f15de8009f4bd63d\nsize 18\n"),
			Oid:  lfsPointer3,
		},
		lfsPointer4: &gitalypb.LFSPointer{
			Size: 129,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:47997ea7ecff33be61e3ca1cc287ee72a2125161518f1a169f2893a5a82e9d95\nsize 7501\n"),
			Oid:  lfsPointer4,
		},
		lfsPointer5: &gitalypb.LFSPointer{
			Size: 129,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:8c1e8de917525f83104736f6c64d32f0e2a02f5bf2ee57843a54f222cba8c813\nsize 2797\n"),
			Oid:  lfsPointer5,
		},
		lfsPointer6: &gitalypb.LFSPointer{
			Size: 132,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:96f74c6fe7a2979eefb9ec74a5dfc6888fb25543cf99b77586b79afea1da6f97\nsize 1219696\n"),
			Oid:  lfsPointer6,
		},
	}
)

func TestListLFSPointers(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.LFSPointersPipeline,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testListLFSPointers(t, ctx)
	})
}

func testListLFSPointers(t *testing.T, ctx context.Context) {
	_, repo, _, client := setup(t)

	for _, tc := range []struct {
		desc             string
		revs             []string
		limit            int32
		expectedPointers []*gitalypb.LFSPointer
		expectedErr      error
	}{
		{
			desc:        "missing revisions",
			revs:        []string{},
			expectedErr: status.Error(codes.InvalidArgument, "missing revisions"),
		},
		{
			desc:        "invalid revision",
			revs:        []string{"-dashed"},
			expectedErr: status.Error(codes.InvalidArgument, "invalid revision: \"-dashed\""),
		},
		{
			desc: "object IDs",
			revs: []string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
				"d5b560e9c17384cf8257347db63167b54e0c97ff", // tree
				"60ecb67744cb56576c30214ff52294f8ce2def98", // commit
			},
			expectedPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
			},
		},
		{
			desc: "revision",
			revs: []string{"refs/heads/master"},
			expectedPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
			},
		},
		{
			desc: "pseudo-revisions",
			revs: []string{"refs/heads/master", "--not", "--all"},
		},
		{
			desc: "partial graph walk",
			revs: []string{"--all", "--not", "refs/heads/master"},
			expectedPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
				lfsPointers[lfsPointer4],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
		},
		{
			desc:  "partial graph walk with matching limit",
			revs:  []string{"--all", "--not", "refs/heads/master"},
			limit: 5,
			expectedPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
				lfsPointers[lfsPointer4],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
		},
		{
			desc:  "partial graph walk with limiting limit",
			revs:  []string{"--all", "--not", "refs/heads/master"},
			limit: 3,
			expectedPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer4],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.ListLFSPointers(ctx, &gitalypb.ListLFSPointersRequest{
				Repository: repo,
				Revisions:  tc.revs,
				Limit:      tc.limit,
			})
			require.NoError(t, err)

			var actualLFSPointers []*gitalypb.LFSPointer
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				testassert.GrpcEqualErr(t, tc.expectedErr, err)
				if err != nil {
					break
				}

				actualLFSPointers = append(actualLFSPointers, resp.GetLfsPointers()...)
			}
			lfsPointersEqual(t, tc.expectedPointers, actualLFSPointers)
		})
	}
}

func TestListAllLFSPointers(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.LFSPointersPipeline,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testListAllLFSPointers(t, ctx)
	})
}

func testListAllLFSPointers(t *testing.T, ctx context.Context) {
	receivePointers := func(t *testing.T, stream gitalypb.BlobService_ListAllLFSPointersClient) []*gitalypb.LFSPointer {
		t.Helper()

		var pointers []*gitalypb.LFSPointer
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.Nil(t, err)
			pointers = append(pointers, resp.GetLfsPointers()...)
		}
		return pointers
	}

	lfsPointerContents := `version https://git-lfs.github.com/spec/v1
oid sha256:1111111111111111111111111111111111111111111111111111111111111111
size 12345`

	t.Run("normal repository", func(t *testing.T) {
		_, repo, _, client := setup(t)
		stream, err := client.ListAllLFSPointers(ctx, &gitalypb.ListAllLFSPointersRequest{
			Repository: repo,
		})
		require.NoError(t, err)
		lfsPointersEqual(t, []*gitalypb.LFSPointer{
			lfsPointers[lfsPointer1],
			lfsPointers[lfsPointer2],
			lfsPointers[lfsPointer3],
			lfsPointers[lfsPointer4],
			lfsPointers[lfsPointer5],
			lfsPointers[lfsPointer6],
		}, receivePointers(t, stream))
	})

	t.Run("dangling LFS pointer", func(t *testing.T) {
		cfg, repo, repoPath, client := setup(t)

		hash := gittest.ExecStream(t, cfg, strings.NewReader(lfsPointerContents), "-C", repoPath, "hash-object", "-w", "--stdin")
		lfsPointerOID := text.ChompBytes(hash)

		stream, err := client.ListAllLFSPointers(ctx, &gitalypb.ListAllLFSPointersRequest{
			Repository: repo,
		})
		require.NoError(t, err)
		lfsPointersEqual(t, []*gitalypb.LFSPointer{
			&gitalypb.LFSPointer{
				Oid:  lfsPointerOID,
				Data: []byte(lfsPointerContents),
				Size: int64(len(lfsPointerContents)),
			},
			lfsPointers[lfsPointer1],
			lfsPointers[lfsPointer2],
			lfsPointers[lfsPointer3],
			lfsPointers[lfsPointer4],
			lfsPointers[lfsPointer5],
			lfsPointers[lfsPointer6],
		}, receivePointers(t, stream))
	})

	t.Run("quarantine", func(t *testing.T) {
		cfg, repoProto, repoPath, client := setup(t)

		// We're emulating the case where git is receiving data via a push, where objects
		// are stored in a separate quarantine environment. In this case, LFS pointer checks
		// may want to inspect all newly pushed objects, denoted by a repository proto
		// message which only has its object directory set to the quarantine directory.
		quarantineDir := "objects/incoming-123456"
		require.NoError(t, os.Mkdir(filepath.Join(repoPath, quarantineDir), 0777))
		repoProto.GitObjectDirectory = quarantineDir
		repoProto.GitAlternateObjectDirectories = nil

		// There are no quarantined objects yet, so none should be returned here.
		stream, err := client.ListAllLFSPointers(ctx, &gitalypb.ListAllLFSPointersRequest{
			Repository: repoProto,
		})
		require.NoError(t, err)
		require.Empty(t, receivePointers(t, stream))

		// Write a new object into the repository. Because we set GIT_OBJECT_DIRECTORY to
		// the quarantine directory, objects will be written in there instead of into the
		// repository's normal object directory.
		repo := localrepo.NewTestRepo(t, cfg, repoProto)
		var buffer, stderr bytes.Buffer
		err = repo.ExecAndWait(ctx, git.SubCmd{
			Name: "hash-object",
			Flags: []git.Option{
				git.Flag{Name: "-w"},
				git.Flag{Name: "--stdin"},
			},
		}, git.WithStdin(strings.NewReader(lfsPointerContents)), git.WithStdout(&buffer), git.WithStderr(&stderr))
		require.NoError(t, err)

		stream, err = client.ListAllLFSPointers(ctx, &gitalypb.ListAllLFSPointersRequest{
			Repository: repoProto,
		})
		require.NoError(t, err)

		// We only expect to find a single LFS pointer, which is the one we've just written
		// into the quarantine directory.
		lfsPointersEqual(t, []*gitalypb.LFSPointer{
			&gitalypb.LFSPointer{
				Oid:  text.ChompBytes(buffer.Bytes()),
				Data: []byte(lfsPointerContents),
				Size: int64(len(lfsPointerContents)),
			},
		}, receivePointers(t, stream))
	})
}

func TestSuccessfulGetLFSPointersRequest(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.LFSPointersPipeline,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testSuccessfulGetLFSPointersRequest(t, ctx)
	})
}

func testSuccessfulGetLFSPointersRequest(t *testing.T, ctx context.Context) {
	_, repo, _, client := setup(t)

	lfsPointerIds := []string{
		lfsPointer1,
		lfsPointer2,
		lfsPointer3,
	}
	otherObjectIds := []string{
		"d5b560e9c17384cf8257347db63167b54e0c97ff", // tree
		"60ecb67744cb56576c30214ff52294f8ce2def98", // commit
	}

	expectedLFSPointers := []*gitalypb.LFSPointer{
		lfsPointers[lfsPointer1],
		lfsPointers[lfsPointer2],
		lfsPointers[lfsPointer3],
	}

	request := &gitalypb.GetLFSPointersRequest{
		Repository: repo,
		BlobIds:    append(lfsPointerIds, otherObjectIds...),
	}

	stream, err := client.GetLFSPointers(ctx, request)
	require.NoError(t, err)

	var receivedLFSPointers []*gitalypb.LFSPointer
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		receivedLFSPointers = append(receivedLFSPointers, resp.GetLfsPointers()...)
	}

	lfsPointersEqual(t, receivedLFSPointers, expectedLFSPointers)
}

func TestFailedGetLFSPointersRequestDueToValidations(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.LFSPointersPipeline,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testFailedGetLFSPointersRequestDueToValidations(t, ctx)
	})
}

func testFailedGetLFSPointersRequestDueToValidations(t *testing.T, ctx context.Context) {
	_, repo, _, client := setup(t)

	testCases := []struct {
		desc    string
		request *gitalypb.GetLFSPointersRequest
		code    codes.Code
	}{
		{
			desc: "empty Repository",
			request: &gitalypb.GetLFSPointersRequest{
				Repository: nil,
				BlobIds:    []string{"f00"},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty BlobIds",
			request: &gitalypb.GetLFSPointersRequest{
				Repository: repo,
				BlobIds:    nil,
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			stream, err := client.GetLFSPointers(ctx, testCase.request)
			require.NoError(t, err)

			_, err = stream.Recv()
			require.NotEqual(t, io.EOF, err)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestFindLFSPointersByRevisions(t *testing.T) {
	cfg := testcfg.Build(t)

	gitCmdFactory := git.NewExecCommandFactory(cfg)

	repoProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)
	repo := localrepo.NewTestRepo(t, cfg, repoProto)

	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range []struct {
		desc                string
		revs                []string
		limit               int
		expectedErr         error
		expectedLFSPointers []*gitalypb.LFSPointer
	}{
		{
			desc: "--all",
			revs: []string{"--all"},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
				lfsPointers[lfsPointer4],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
		},
		{
			desc:  "--all with high limit",
			revs:  []string{"--all"},
			limit: 7,
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
				lfsPointers[lfsPointer4],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
		},
		{
			desc:  "--all with truncating limit",
			revs:  []string{"--all"},
			limit: 3,
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
			expectedErr: errLimitReached,
		},
		{
			desc: "--not --all",
			revs: []string{"--not", "--all"},
		},
		{
			desc: "initial commit",
			revs: []string{"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"},
		},
		{
			desc: "master",
			revs: []string{"master"},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
			},
		},
		{
			desc: "multiple revisions",
			revs: []string{"master", "moar-lfs-ptrs"},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
			},
		},
		{
			desc:        "invalid revision",
			revs:        []string{"does-not-exist"},
			expectedErr: fmt.Errorf("fatal: ambiguous argument 'does-not-exist'"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var collector lfsPointerCollector

			err := findLFSPointersByRevisions(ctx, repo, gitCmdFactory,
				collector.chunker(), tc.limit, tc.revs...)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), tc.expectedErr.Error())
			}
			lfsPointersEqual(t, tc.expectedLFSPointers, collector.pointers)
		})
	}
}

func BenchmarkFindLFSPointers(b *testing.B) {
	cfg := testcfg.Build(b)

	gitCmdFactory := git.NewExecCommandFactory(cfg)

	repoProto, _, cleanup := gittest.CloneBenchRepo(b, cfg)
	b.Cleanup(cleanup)
	repo := localrepo.NewTestRepo(b, cfg, repoProto)

	ctx, cancel := testhelper.Context()
	defer cancel()

	b.Run("limitless", func(b *testing.B) {
		var collector lfsPointerCollector
		err := findLFSPointersByRevisions(ctx, repo, gitCmdFactory, collector.chunker(), 0, "--all")
		require.NoError(b, err)
	})

	b.Run("limit", func(b *testing.B) {
		var collector lfsPointerCollector
		err := findLFSPointersByRevisions(ctx, repo, gitCmdFactory, collector.chunker(), 1, "--all")
		require.NoError(b, err)
		require.Len(b, collector.pointers, 1)
	})
}

func BenchmarkReadLFSPointers(b *testing.B) {
	cfg := testcfg.Build(b)

	repoProto, path, cleanup := gittest.CloneBenchRepo(b, cfg)
	b.Cleanup(cleanup)
	repo := localrepo.NewTestRepo(b, cfg, repoProto)

	ctx, cancel := testhelper.Context()
	defer cancel()

	candidates := gittest.Exec(b, cfg, "-C", path, "rev-list", "--in-commit-order", "--objects", "--no-object-names", "--filter=blob:limit=200", "--all")

	b.Run("limitless", func(b *testing.B) {
		var collector lfsPointerCollector
		err := readLFSPointers(ctx, repo, collector.chunker(), bytes.NewReader(candidates), 0)
		require.NoError(b, err)
	})

	b.Run("limit", func(b *testing.B) {
		var collector lfsPointerCollector
		err := readLFSPointers(ctx, repo, collector.chunker(), bytes.NewReader(candidates), 1)
		require.Equal(b, errLimitReached, err)
		require.Equal(b, 1, len(collector.pointers))
	})
}

func TestReadLFSPointers(t *testing.T) {
	cfg, repo, _, _ := setup(t)

	localRepo := localrepo.NewTestRepo(t, cfg, repo)

	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range []struct {
		desc                string
		input               string
		limit               int
		expectedErr         error
		expectedLFSPointers []*gitalypb.LFSPointer
	}{
		{
			desc:  "single object ID",
			input: strings.Join([]string{lfsPointer1}, "\n"),
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
			},
		},
		{
			desc: "multiple object IDs",
			input: strings.Join([]string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
				lfsPointer4,
				lfsPointer5,
				lfsPointer6,
			}, "\n"),
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
				lfsPointers[lfsPointer4],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
		},
		{
			desc: "multiple object IDs with high limit",
			input: strings.Join([]string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
				lfsPointer4,
				lfsPointer5,
				lfsPointer6,
			}, "\n"),
			limit: 7,
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
				lfsPointers[lfsPointer4],
				lfsPointers[lfsPointer5],
				lfsPointers[lfsPointer6],
			},
		},
		{
			desc: "multiple object IDs with truncating limit",
			input: strings.Join([]string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3,
				lfsPointer4,
				lfsPointer5,
				lfsPointer6,
			}, "\n"),
			limit: 3,
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
			},
			expectedErr: errLimitReached,
		},
		{
			desc: "multiple object IDs with name filter",
			input: strings.Join([]string{
				lfsPointer1,
				lfsPointer2,
				lfsPointer3 + " x",
				lfsPointer4,
				lfsPointer5 + " z",
				lfsPointer6 + " a",
			}, "\n"),
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
			},
			expectedErr: errors.New("object not found"),
		},
		{
			desc: "non-pointer object",
			input: strings.Join([]string{
				"60ecb67744cb56576c30214ff52294f8ce2def98",
			}, "\n"),
		},
		{
			desc: "mixed objects",
			input: strings.Join([]string{
				"60ecb67744cb56576c30214ff52294f8ce2def98",
				lfsPointer2,
			}, "\n"),
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer2],
			},
		},
		{
			desc: "missing object",
			input: strings.Join([]string{
				"0101010101010101010101010101010101010101",
			}, "\n"),
			expectedErr: errors.New("object not found"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			reader := strings.NewReader(tc.input)

			var collector lfsPointerCollector

			err := readLFSPointers(ctx, localRepo, collector.chunker(), reader, tc.limit)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), tc.expectedErr.Error())
			}

			lfsPointersEqual(t, tc.expectedLFSPointers, collector.pointers)
		})
	}
}

func lfsPointersEqual(tb testing.TB, expected, actual []*gitalypb.LFSPointer) {
	tb.Helper()

	for _, slice := range [][]*gitalypb.LFSPointer{expected, actual} {
		sort.Slice(slice, func(i, j int) bool {
			return strings.Compare(slice[i].Oid, slice[j].Oid) < 0
		})
	}

	require.Equal(tb, len(expected), len(actual))
	for i := range expected {
		testassert.ProtoEqual(tb, expected[i], actual[i])
	}
}

type lfsPointerCollector struct {
	pointers []*gitalypb.LFSPointer
}

func (c *lfsPointerCollector) Append(m proto.Message) {
	c.pointers = append(c.pointers, m.(*gitalypb.LFSPointer))
}

func (c *lfsPointerCollector) Reset() {
	// We don'c reset anything given that we want to collect all pointers.
}

func (c *lfsPointerCollector) Send() error {
	// And neither do we anything here.
	return nil
}

func (c *lfsPointerCollector) chunker() *chunk.Chunker {
	return chunk.New(c)
}
