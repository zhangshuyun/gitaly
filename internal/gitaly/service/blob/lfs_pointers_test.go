package blob

import (
	"io"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulGetLFSPointersRequest(t *testing.T) {
	stop, serverSocketPath := runBlobServer(t, testhelper.DefaultLocator())
	defer stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	lfsPointerIds := []string{
		"0c304a93cb8430108629bbbcaa27db3343299bc0",
		"f78df813119a79bfbe0442ab92540a61d3ab7ff3",
		"bab31d249f78fba464d1b75799aad496cc07fa3b",
	}
	otherObjectIds := []string{
		"d5b560e9c17384cf8257347db63167b54e0c97ff", // tree
		"60ecb67744cb56576c30214ff52294f8ce2def98", // commit
	}

	expectedLFSPointers := []*gitalypb.LFSPointer{
		{
			Size: 133,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:91eff75a492a3ed0dfcb544d7f31326bc4014c8551849c192fd1e48d4dd2c897\nsize 1575078\n\n"),
			Oid:  "0c304a93cb8430108629bbbcaa27db3343299bc0",
		},
		{
			Size: 127,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:f2b0a1e7550e9b718dafc9b525a04879a766de62e4fbdfc46593d47f7ab74636\nsize 20\n"),
			Oid:  "f78df813119a79bfbe0442ab92540a61d3ab7ff3",
		},
		{
			Size: 127,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:bad71f905b60729f502ca339f7c9f001281a3d12c68a5da7f15de8009f4bd63d\nsize 18\n"),
			Oid:  "bab31d249f78fba464d1b75799aad496cc07fa3b",
		},
	}

	request := &gitalypb.GetLFSPointersRequest{
		Repository: testRepo,
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
	stop, serverSocketPath := runBlobServer(t, testhelper.DefaultLocator())
	defer stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

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
				Repository: testRepo,
				BlobIds:    nil,
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.GetLFSPointers(ctx, testCase.request)
			require.NoError(t, err)

			_, err = stream.Recv()
			require.NotEqual(t, io.EOF, err)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestSuccessfulGetNewLFSPointersRequest(t *testing.T) {
	stop, serverSocketPath := runBlobServer(t, testhelper.DefaultLocator())
	defer stop()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepoWithWorktree(t)
	defer cleanupFn()

	revision := []byte("46abbb087fcc0fd02c340f0f2f052bd2c7708da3")
	commiterArgs := []string{"-c", "user.name=Scrooge McDuck", "-c", "user.email=scrooge@mcduck.com"}
	cmdArgs := append(commiterArgs, "-C", testRepoPath, "cherry-pick", string(revision))
	cmd := exec.Command(config.Config.Git.BinPath, cmdArgs...)
	// Skip smudge since it doesn't work with file:// remotes and we don't need it
	cmd.Env = append(cmd.Env, "GIT_LFS_SKIP_SMUDGE=1")
	altDirs := "./alt-objects"
	altDirsCommit := testhelper.CreateCommitInAlternateObjectDirectory(t, config.Config.Git.BinPath, testRepoPath, altDirs, cmd)

	// Create a commit not pointed at by any ref to emulate being in the
	// pre-receive hook so that `--not --all` returns some objects
	newRevision := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "commit-tree", "8856a329dd38ca86dfb9ce5aa58a16d88cc119bd", "-m", "Add LFS objects")
	newRevision = newRevision[:len(newRevision)-1] // Strip newline

	testCases := []struct {
		desc                string
		request             *gitalypb.GetNewLFSPointersRequest
		expectedLFSPointers []*gitalypb.LFSPointer
	}{
		{
			desc: "standard request",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: testRepo,
				Revision:   revision,
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				{
					Size: 133,
					Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:91eff75a492a3ed0dfcb544d7f31326bc4014c8551849c192fd1e48d4dd2c897\nsize 1575078\n\n"),
					Oid:  "0c304a93cb8430108629bbbcaa27db3343299bc0",
				},
				{
					Size: 127,
					Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:f2b0a1e7550e9b718dafc9b525a04879a766de62e4fbdfc46593d47f7ab74636\nsize 20\n"),
					Oid:  "f78df813119a79bfbe0442ab92540a61d3ab7ff3",
				},
				{
					Size: 127,
					Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:bad71f905b60729f502ca339f7c9f001281a3d12c68a5da7f15de8009f4bd63d\nsize 18\n"),
					Oid:  "bab31d249f78fba464d1b75799aad496cc07fa3b",
				},
			},
		},
		{
			desc: "request with revision in alternate directory",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: testRepo,
				Revision:   altDirsCommit,
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				{
					Size: 133,
					Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:91eff75a492a3ed0dfcb544d7f31326bc4014c8551849c192fd1e48d4dd2c897\nsize 1575078\n\n"),
					Oid:  "0c304a93cb8430108629bbbcaa27db3343299bc0",
				},
				{
					Size: 127,
					Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:f2b0a1e7550e9b718dafc9b525a04879a766de62e4fbdfc46593d47f7ab74636\nsize 20\n"),
					Oid:  "f78df813119a79bfbe0442ab92540a61d3ab7ff3",
				},
				{
					Size: 127,
					Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:bad71f905b60729f502ca339f7c9f001281a3d12c68a5da7f15de8009f4bd63d\nsize 18\n"),
					Oid:  "bab31d249f78fba464d1b75799aad496cc07fa3b",
				},
			},
		},
		{
			desc: "request with limit",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: testRepo,
				Revision:   revision,
				// This is limiting the amount of lines processed from the
				// output of rev-list. For example, for this revision's  output
				// there's an LFS object id in line 4 and another in line 14, so
				// any limit in [0, 3] will yield no pointers, [4,13] will yield
				// one, and [14,] will yield two. This is weird but it's the
				// way the current implementation works ¯\_(ツ)_/¯
				Limit: 19,
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
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
			desc: "with NotInAll true",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: testRepo,
				Revision:   newRevision,
				NotInAll:   true,
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
				{
					Size: 133,
					Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:91eff75a492a3ed0dfcb544d7f31326bc4014c8551849c192fd1e48d4dd2c897\nsize 1575078\n\n"),
					Oid:  "0c304a93cb8430108629bbbcaa27db3343299bc0",
				},
			},
		},
		{
			desc: "with some NotInRefs elements",
			request: &gitalypb.GetNewLFSPointersRequest{
				Repository: testRepo,
				Revision:   revision,
				NotInRefs:  [][]byte{[]byte("048721d90c449b244b7b4c53a9186b04330174ec")},
			},
			expectedLFSPointers: []*gitalypb.LFSPointer{
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
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

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
	stop, serverSocketPath := runBlobServer(t, testhelper.DefaultLocator())
	defer stop()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

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
			repository: testRepo,
			revision:   nil,
		},
		{
			desc:       "revision can't start with '-'",
			repository: testRepo,
			revision:   []byte("-suspicious-revision"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request := &gitalypb.GetNewLFSPointersRequest{
				Repository: tc.repository,
				Revision:   tc.revision,
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

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

func TestSuccessfulGetAllLFSPointersRequest(t *testing.T) {
	stop, serverSocketPath := runBlobServer(t, testhelper.DefaultLocator())
	defer stop()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	request := &gitalypb.GetAllLFSPointersRequest{
		Repository: testRepo,
	}

	expectedLFSPointers := []*gitalypb.LFSPointer{
		{
			Size: 133,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:91eff75a492a3ed0dfcb544d7f31326bc4014c8551849c192fd1e48d4dd2c897\nsize 1575078\n\n"),
			Oid:  "0c304a93cb8430108629bbbcaa27db3343299bc0",
		},
		{
			Size: 127,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:f2b0a1e7550e9b718dafc9b525a04879a766de62e4fbdfc46593d47f7ab74636\nsize 20\n"),
			Oid:  "f78df813119a79bfbe0442ab92540a61d3ab7ff3",
		},
		{
			Size: 127,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:bad71f905b60729f502ca339f7c9f001281a3d12c68a5da7f15de8009f4bd63d\nsize 18\n"),
			Oid:  "bab31d249f78fba464d1b75799aad496cc07fa3b",
		},
		{
			Size: 132,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:96f74c6fe7a2979eefb9ec74a5dfc6888fb25543cf99b77586b79afea1da6f97\nsize 1219696\n"),
			Oid:  "ff0ab3afd1616ff78d0331865d922df103b64cf0",
		},
		{
			Size: 129,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:8c1e8de917525f83104736f6c64d32f0e2a02f5bf2ee57843a54f222cba8c813\nsize 2797\n"),
			Oid:  "0360724a0d64498331888f1eaef2d24243809230",
		},
		{
			Size: 129,
			Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:47997ea7ecff33be61e3ca1cc287ee72a2125161518f1a169f2893a5a82e9d95\nsize 7501\n"),
			Oid:  "125fcc9f6e33175cb278b9b2809154d2535fe19f",
		},
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
	stop, serverSocketPath := runBlobServer(t, testhelper.DefaultLocator())
	defer stop()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

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

// TestGetAllLFSPointersVerifyScope verifies that this RPC returns all LFS
// pointers in a repository, not only ones reachable from the default branch
func TestGetAllLFSPointersVerifyScope(t *testing.T) {
	stop, serverSocketPath := runBlobServer(t, testhelper.DefaultLocator())
	defer stop()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, repoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	request := &gitalypb.GetAllLFSPointersRequest{
		Repository: testRepo,
	}

	c, err := client.GetAllLFSPointers(ctx, request)
	require.NoError(t, err)

	lfsPtr := &gitalypb.LFSPointer{
		Size: 127,
		Data: []byte("version https://git-lfs.github.com/spec/v1\noid sha256:f2b0a1e7550e9b718dafc9b525a04879a766de62e4fbdfc46593d47f7ab74636\nsize 20\n"),
		Oid:  "f78df813119a79bfbe0442ab92540a61d3ab7ff3",
	}

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
