package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
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

func testSuccessfulGetLFSPointersRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoGetLFSPointers,
		featureflag.LFSPointersUseBitmapIndex,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testSuccessfulGetLFSPointersRequestFeatured(t, ctx, cfg, rubySrv)
	})
}

func testSuccessfulGetLFSPointersRequestFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	repo, _, client := setupWithRuby(t, cfg, rubySrv)

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

	require.ElementsMatch(t, receivedLFSPointers, expectedLFSPointers)
}

func TestFailedGetLFSPointersRequestDueToValidations(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoGetLFSPointers,
		featureflag.LFSPointersUseBitmapIndex,
	}).Run(t, testFailedGetLFSPointersRequestDueToValidations)
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

func testSuccessfulGetNewLFSPointersRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoGetNewLFSPointers,
		featureflag.LFSPointersUseBitmapIndex,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testSuccessfulGetNewLFSPointersRequestFeatured(t, ctx, cfg, rubySrv)
	})
}

func testSuccessfulGetNewLFSPointersRequestFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	_, _, client := setupWithRuby(t, cfg, rubySrv)

	repo, repoPath, cleanup := gittest.CloneRepoWithWorktreeAtStorage(t, cfg.Storages[0])
	t.Cleanup(cleanup)

	revision := []byte("46abbb087fcc0fd02c340f0f2f052bd2c7708da3")
	commiterArgs := []string{"-c", "user.name=Scrooge McDuck", "-c", "user.email=scrooge@mcduck.com"}
	cmdArgs := append(commiterArgs, "-C", repoPath, "cherry-pick", string(revision))
	cmd := exec.Command(cfg.Git.BinPath, cmdArgs...)
	// Skip smudge since it doesn't work with file:// remotes and we don't need it
	cmd.Env = append(cmd.Env, "GIT_LFS_SKIP_SMUDGE=1")
	altDirs := "./alt-objects"
	altDirsCommit := gittest.CreateCommitInAlternateObjectDirectory(t, cfg.Git.BinPath, repoPath, altDirs, cmd)

	// Create a commit not pointed at by any ref to emulate being in the
	// pre-receive hook so that `--not --all` returns some objects
	newRevision := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "commit-tree", "8856a329dd38ca86dfb9ce5aa58a16d88cc119bd", "-m", "Add LFS objects")
	newRevision = newRevision[:len(newRevision)-1] // Strip newline

	testCases := []struct {
		desc                string
		request             *gitalypb.GetNewLFSPointersRequest
		expectedLFSPointers []*gitalypb.LFSPointer
	}{
		{
			desc: "standard request",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: repo,
				Revision:   revision,
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
			},
		},
		{
			desc: "request with revision in alternate directory",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: repo,
				Revision:   altDirsCommit,
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
				lfsPointers[lfsPointer2],
				lfsPointers[lfsPointer3],
			},
		},
		{
			desc: "request with non-exceeding limit",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: repo,
				Revision:   revision,
				Limit:      9000,
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				{
					Size: 133,
					Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:91eff75a492a3ed0dfcb544d7f31326bc4014c8551849c192fd1e48d4dd2c897\nsize 1575078\n\n"),
					Oid:  "0c304a93cb8430108629bbbcaa27db3343299bc0",
				},
				{
					Size: 127,
					Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:bad71f905b60729f502ca339f7c9f001281a3d12c68a5da7f15de8009f4bd63d\nsize 18\n"),
					Oid:  "bab31d249f78fba464d1b75799aad496cc07fa3b",
				},
				{
					Size: 127,
					Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:f2b0a1e7550e9b718dafc9b525a04879a766de62e4fbdfc46593d47f7ab74636\nsize 20\n"),
					Oid:  "f78df813119a79bfbe0442ab92540a61d3ab7ff3",
				},
			},
		},
		{
			desc: "request with smaller limit",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: repo,
				Revision:   revision,
				Limit:      2,
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer3],
				lfsPointers[lfsPointer2],
			},
		},
		{
			desc: "with NotInAll true",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: repo,
				Revision:   newRevision,
				NotInAll:   true,
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer1],
			},
		},
		{
			desc: "with some NotInRefs elements",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: repo,
				Revision:   revision,
				NotInRefs:  [][]byte{[]byte("048721d90c449b244b7b4c53a9186b04330174ec")},
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				lfsPointers[lfsPointer3],
				lfsPointers[lfsPointer2],
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tc.request.Repository.GitAlternateObjectDirectories = []string{altDirs}
			stream, err := client.GetNewLFSPointers(ctx, tc.request)
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

			require.ElementsMatch(t, receivedLFSPointers, tc.expectedLFSPointers)
		})
	}
}

func TestFailedGetNewLFSPointersRequestDueToValidations(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoGetNewLFSPointers,
		featureflag.LFSPointersUseBitmapIndex,
	}).Run(t, testFailedGetNewLFSPointersRequestDueToValidations)
}

func testFailedGetNewLFSPointersRequestDueToValidations(t *testing.T, ctx context.Context) {
	_, repo, _, client := setup(t)

	testCases := []struct {
		desc       string
		repository *gitalypb.Repository
		revision   []byte
	}{
		{
			desc:       "empty Repository",
			repository: nil,
			revision:   []byte("master"),
		},
		{
			desc:       "empty revision",
			repository: repo,
			revision:   nil,
		},
		{
			desc:       "revision can't start with '-'",
			repository: repo,
			revision:   []byte("-suspicious-revision"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request := &gitalypb.GetNewLFSPointersRequest{
				Repository: tc.repository,
				Revision:   tc.revision,
			}

			c, err := client.GetNewLFSPointers(ctx, request)
			require.NoError(t, err)

			err = drainNewPointers(c)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
		})
	}
}

func drainNewPointers(c gitalypb.BlobService_GetNewLFSPointersClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}
}

func testSuccessfulGetAllLFSPointersRequest(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoGetAllLFSPointers,
		featureflag.LFSPointersUseBitmapIndex,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testSuccessfulGetAllLFSPointersRequestFeatured(t, ctx, cfg, rubySrv)
	})
}

func testSuccessfulGetAllLFSPointersRequestFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	repo, _, client := setupWithRuby(t, cfg, rubySrv)

	request := &gitalypb.GetAllLFSPointersRequest{
		Repository: repo,
	}

	expectedLFSPointers := []*gitalypb.LFSPointer{
		lfsPointers[lfsPointer1],
		lfsPointers[lfsPointer2],
		lfsPointers[lfsPointer3],
		lfsPointers[lfsPointer4],
		lfsPointers[lfsPointer5],
		lfsPointers[lfsPointer6],
	}

	c, err := client.GetAllLFSPointers(ctx, request)
	require.NoError(t, err)

	require.ElementsMatch(t, expectedLFSPointers, getAllPointers(t, c))
}

func getAllPointers(t *testing.T, c gitalypb.BlobService_GetAllLFSPointersClient) []*gitalypb.LFSPointer {
	var receivedLFSPointers []*gitalypb.LFSPointer
	for {
		resp, err := c.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		receivedLFSPointers = append(receivedLFSPointers, resp.GetLfsPointers()...)
	}

	return receivedLFSPointers
}

func TestFailedGetAllLFSPointersRequestDueToValidations(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoGetAllLFSPointers,
		featureflag.LFSPointersUseBitmapIndex,
	}).Run(t, testFailedGetAllLFSPointersRequestDueToValidations)
}

func testFailedGetAllLFSPointersRequestDueToValidations(t *testing.T, ctx context.Context) {
	_, _, _, client := setup(t)

	testCases := []struct {
		desc       string
		repository *gitalypb.Repository
	}{
		{
			desc:       "empty Repository",
			repository: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request := &gitalypb.GetAllLFSPointersRequest{
				Repository: tc.repository,
			}

			c, err := client.GetAllLFSPointers(ctx, request)
			require.NoError(t, err)

			err = drainAllPointers(c)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
			require.Contains(t, err.Error(), tc.desc)
		})
	}
}

func drainAllPointers(c gitalypb.BlobService_GetAllLFSPointersClient) error {
	for {
		_, err := c.Recv()
		if err != nil {
			return err
		}
	}
}

func testGetAllLFSPointersVerifyScope(t *testing.T, cfg config.Cfg, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoGetAllLFSPointers,
		featureflag.LFSPointersUseBitmapIndex,
	}).Run(t, func(t *testing.T, ctx context.Context) {
		testGetAllLFSPointersVerifyScopeFeatured(t, ctx, cfg, rubySrv)
	})
}

// TestGetAllLFSPointersVerifyScope verifies that this RPC returns all LFS
// pointers in a repository, not only ones reachable from the default branch
func testGetAllLFSPointersVerifyScopeFeatured(t *testing.T, ctx context.Context, cfg config.Cfg, rubySrv *rubyserver.Server) {
	repo, repoPath, client := setupWithRuby(t, cfg, rubySrv)

	request := &gitalypb.GetAllLFSPointersRequest{
		Repository: repo,
	}

	c, err := client.GetAllLFSPointers(ctx, request)
	require.NoError(t, err)

	lfsPtr := lfsPointers[lfsPointer2]

	// the LFS pointer is reachable from a non-default branch:
	require.True(t, refHasPtr(t, repoPath, "moar-lfs-ptrs", lfsPtr))

	// the same pointer is not reachable from a default branch
	require.False(t, refHasPtr(t, repoPath, "master", lfsPtr))

	require.Contains(t, getAllPointers(t, c), lfsPtr,
		"RPC should return all LFS pointers, not just ones in the default branch")
}

// refHasPtr verifies the provided ref has connectivity to the LFS pointer
func refHasPtr(t *testing.T, repoPath, ref string, lfsPtr *gitalypb.LFSPointer) bool {
	objects := string(testhelper.MustRunCommand(t, nil,
		"git", "-C", repoPath, "rev-list", "--objects", ref))

	return strings.Contains(objects, lfsPtr.Oid)
}

func TestFindLFSPointersByRevisions(t *testing.T) {
	cfg := testcfg.Build(t)

	gitCmdFactory := git.NewExecCommandFactory(cfg)

	repoProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)
	repo := localrepo.New(gitCmdFactory, repoProto, cfg)

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
			desc:        "invalid dashed option",
			revs:        []string{"master", "--foobar"},
			expectedErr: fmt.Errorf("invalid revision: \"--foobar\""),
		},
		{
			desc:        "invalid revision",
			revs:        []string{"does-not-exist"},
			expectedErr: fmt.Errorf("fatal: ambiguous argument 'does-not-exist'"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actualLFSPointers, err := findLFSPointersByRevisions(
				ctx, repo, gitCmdFactory, tc.limit, tc.revs...)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), tc.expectedErr.Error())
			}
			require.ElementsMatch(t, tc.expectedLFSPointers, actualLFSPointers)
		})
	}
}

func BenchmarkFindLFSPointers(b *testing.B) {
	cfg := testcfg.Build(b)

	gitCmdFactory := git.NewExecCommandFactory(cfg)

	repoProto, _, cleanup := gittest.CloneBenchRepo(b)
	b.Cleanup(cleanup)
	repo := localrepo.New(gitCmdFactory, repoProto, cfg)

	ctx, cancel := testhelper.Context()
	defer cancel()

	b.Run("limitless", func(b *testing.B) {
		_, err := findLFSPointersByRevisions(ctx, repo, gitCmdFactory, 0, "--all")
		require.NoError(b, err)
	})

	b.Run("limit", func(b *testing.B) {
		lfsPointer, err := findLFSPointersByRevisions(ctx, repo, gitCmdFactory, 1, "--all")
		require.NoError(b, err)
		require.Len(b, lfsPointer, 1)
	})
}

func BenchmarkReadLFSPointers(b *testing.B) {
	cfg := testcfg.Build(b)

	gitCmdFactory := git.NewExecCommandFactory(cfg)

	repoProto, path, cleanup := gittest.CloneBenchRepo(b)
	b.Cleanup(cleanup)
	repo := localrepo.New(gitCmdFactory, repoProto, cfg)

	ctx, cancel := testhelper.Context()
	defer cancel()

	candidates := testhelper.MustRunCommand(b, nil, "git", "-C", path, "rev-list", "--in-commit-order", "--objects", "--no-object-names", "--filter=blob:limit=200", "--all")

	b.Run("limitless", func(b *testing.B) {
		_, err := readLFSPointers(ctx, repo, bytes.NewReader(candidates), 0)
		require.NoError(b, err)
	})

	b.Run("limit", func(b *testing.B) {
		lfsPointer, err := readLFSPointers(ctx, repo, bytes.NewReader(candidates), 1)
		require.NoError(b, err)
		require.Len(b, lfsPointer, 1)
	})
}

func TestReadLFSPointers(t *testing.T) {
	cfg, repo, _, _ := setup(t)

	gitCmdFactory := git.NewExecCommandFactory(cfg)

	localRepo := localrepo.New(gitCmdFactory, repo, cfg)

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

			actualLFSPointers, err := readLFSPointers(
				ctx, localRepo, reader, tc.limit)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), tc.expectedErr.Error())
			}
			require.ElementsMatch(t, tc.expectedLFSPointers, actualLFSPointers)
		})
	}
}

func TestSliceLFSPointers(t *testing.T) {
	generateSlice := func(n, offset int) []*gitalypb.LFSPointer {
		slice := make([]*gitalypb.LFSPointer, n)
		for i := 0; i < n; i++ {
			slice[i] = &gitalypb.LFSPointer{
				Size: int64(i + offset),
			}
		}
		return slice
	}

	for _, tc := range []struct {
		desc           string
		err            error
		lfsPointers    []*gitalypb.LFSPointer
		expectedSlices [][]*gitalypb.LFSPointer
	}{
		{
			desc: "empty",
		},
		{
			desc:        "single slice",
			lfsPointers: generateSlice(10, 0),
			expectedSlices: [][]*gitalypb.LFSPointer{
				generateSlice(10, 0),
			},
		},
		{
			desc:        "two slices",
			lfsPointers: generateSlice(101, 0),
			expectedSlices: [][]*gitalypb.LFSPointer{
				generateSlice(100, 0),
				generateSlice(1, 100),
			},
		},
		{
			desc:        "many slices",
			lfsPointers: generateSlice(635, 0),
			expectedSlices: [][]*gitalypb.LFSPointer{
				generateSlice(100, 0),
				generateSlice(100, 100),
				generateSlice(100, 200),
				generateSlice(100, 300),
				generateSlice(100, 400),
				generateSlice(100, 500),
				generateSlice(35, 600),
			},
		},
		{
			desc:        "error",
			lfsPointers: generateSlice(500, 0),
			err:         errors.New("foo"),
			expectedSlices: [][]*gitalypb.LFSPointer{
				generateSlice(100, 0),
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var slices [][]*gitalypb.LFSPointer

			err := sliceLFSPointers(tc.lfsPointers, func(slice []*gitalypb.LFSPointer) error {
				slices = append(slices, slice)
				return tc.err
			})
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.expectedSlices, slices)
		})
	}
}
