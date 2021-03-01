package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestApplyGitattributesSuccess(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	infoPath := filepath.Join(testhelper.GitlabTestStoragePath(),
		testRepo.GetRelativePath(), "info")
	attributesPath := filepath.Join(infoPath, "attributes")

	tests := []struct {
		desc     string
		revision []byte
		contents []byte
	}{
		{
			desc:     "With a .gitattributes file",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			contents: []byte("/custom-highlighting/*.gitlab-custom gitlab-language=ruby\n"),
		},
		{
			desc:     "Without a .gitattributes file",
			revision: []byte("7efb185dd22fd5c51ef044795d62b7847900c341"),
			contents: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Test when no /info folder exists
			if err := os.RemoveAll(infoPath); err != nil {
				t.Fatal(err)
			}
			assertGitattributesApplied(t, client, testRepo, attributesPath, test.revision, test.contents)

			// Test when no git attributes file exists
			if err := os.Remove(attributesPath); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			assertGitattributesApplied(t, client, testRepo, attributesPath, test.revision, test.contents)

			// Test when a git attributes file already exists
			ioutil.WriteFile(attributesPath, []byte("*.docx diff=word"), 0644)
			assertGitattributesApplied(t, client, testRepo, attributesPath, test.revision, test.contents)
		})
	}
}

type testTransactionServer struct {
	gitalypb.UnimplementedRefTransactionServer
	vote func(*gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error)
}

func (s *testTransactionServer) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	if s.vote != nil {
		return s.vote(in)
	}
	return nil, nil
}

func TestApplyGitattributesWithTransaction(t *testing.T) {
	txManager := transaction.NewManager(config.Config)
	locator := config.NewLocator(config.Config)
	transactionServer := &testTransactionServer{}

	srv := testhelper.NewServerWithAuth(t, nil, nil, config.Config.Auth.Token)
	gitalypb.RegisterRepositoryServiceServer(srv.GrpcServer(), NewServer(config.Config, RubyServer, locator, txManager, git.NewExecCommandFactory(config.Config)))
	gitalypb.RegisterRefTransactionServer(srv.GrpcServer(), transactionServer)
	srv.Start(t)
	defer srv.Stop()

	// We're creating a secondary listener in order to route around
	// Praefect in our tests. Otherwise Praefect would replace our
	// carefully crafted transaction and server information.
	transactionServerListener, err := net.Listen("unix", testhelper.GetTemporaryGitalySocketFileName(t))
	require.NoError(t, err)
	go func() { require.NoError(t, srv.GrpcServer().Serve(transactionServerListener)) }()

	client, conn := newRepositoryClient(t, "unix://"+transactionServerListener.Addr().String())
	defer conn.Close()

	praefect := metadata.PraefectServer{
		SocketPath: "unix://" + transactionServerListener.Addr().String(),
		Token:      config.Config.Auth.Token,
	}

	repo, repoPath, cleanup := gittest.CloneRepo(t)
	defer cleanup()

	for _, tc := range []struct {
		desc        string
		revision    []byte
		voteFn      func(*testing.T, *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error)
		withTx      bool
		shouldExist bool
		expectedErr error
	}{
		{
			desc:     "successful vote writes gitattributes",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			withTx:   true,
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				oid, err := git.NewObjectIDFromHex("36814a3da051159a1683479e7a1487120309db8f")
				require.NoError(t, err)
				hash, err := oid.Bytes()
				require.NoError(t, err)

				require.Equal(t, hash, request.ReferenceUpdatesHash)
				return &gitalypb.VoteTransactionResponse{
					State: gitalypb.VoteTransactionResponse_COMMIT,
				}, nil
			},
			shouldExist: true,
		},
		{
			desc:     "aborted vote does not write gitattributes",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			withTx:   true,
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				return &gitalypb.VoteTransactionResponse{
					State: gitalypb.VoteTransactionResponse_ABORT,
				}, nil
			},
			shouldExist: false,
			expectedErr: status.Error(codes.Unknown, "could not commit gitattributes: vote failed: transaction was aborted"),
		},
		{
			desc:     "failing vote does not write gitattributes",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			withTx:   true,
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				return nil, errors.New("foobar")
			},
			shouldExist: false,
			expectedErr: status.Error(codes.Unknown, "could not commit gitattributes: vote failed: rpc error: code = Unknown desc = foobar"),
		},
		{
			desc:     "commit without gitattributes performs vote",
			withTx:   true,
			revision: []byte("7efb185dd22fd5c51ef044795d62b7847900c341"),
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				require.Equal(t, bytes.Repeat([]byte{0x00}, 20), request.ReferenceUpdatesHash)
				return &gitalypb.VoteTransactionResponse{
					State: gitalypb.VoteTransactionResponse_COMMIT,
				}, nil
			},
			shouldExist: false,
		},
		{
			desc:     "disabled feature flag disables transaction",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
			withTx:   false,
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				return &gitalypb.VoteTransactionResponse{
					State: gitalypb.VoteTransactionResponse_ABORT,
				}, errors.New(" this error shouldn't ever be seen")
			},
			shouldExist: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			infoPath := filepath.Join(repoPath, "info")
			require.NoError(t, os.RemoveAll(infoPath))

			ctx, cancel := testhelper.Context()
			defer cancel()
			ctx, err := metadata.InjectTransaction(ctx, 1, "primary", true)
			require.NoError(t, err)
			ctx, err = praefect.Inject(ctx)
			require.NoError(t, err)
			ctx = helper.IncomingToOutgoing(ctx)
			ctx = featureflag.OutgoingCtxWithFeatureFlagValue(ctx, featureflag.TxApplyGitattributes, fmt.Sprintf("%v", tc.withTx))

			transactionServer.vote = func(request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				return tc.voteFn(t, request)
			}

			_, err = client.ApplyGitattributes(ctx, &gitalypb.ApplyGitattributesRequest{
				Repository: repo,
				Revision:   tc.revision,
			})
			require.Equal(t, tc.expectedErr, err)

			path := filepath.Join(infoPath, "attributes")
			if tc.shouldExist {
				require.FileExists(t, path)
				contents := testhelper.MustReadFile(t, path)
				require.Equal(t, []byte("/custom-highlighting/*.gitlab-custom gitlab-language=ruby\n"), contents)
			} else {
				require.NoFileExists(t, path)
			}
		})
	}
}

func TestApplyGitattributesFailure(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	tests := []struct {
		repo     *gitalypb.Repository
		revision []byte
		code     codes.Code
	}{
		{
			repo:     nil,
			revision: nil,
			code:     codes.InvalidArgument,
		},
		{
			repo:     &gitalypb.Repository{StorageName: "foo"},
			revision: []byte("master"),
			code:     codes.InvalidArgument,
		},
		{
			repo:     &gitalypb.Repository{RelativePath: "bar"},
			revision: []byte("master"),
			code:     codes.InvalidArgument,
		},
		{
			repo:     &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "bar"},
			revision: []byte("master"),
			code:     codes.NotFound,
		},
		{
			repo:     testRepo,
			revision: []byte(""),
			code:     codes.InvalidArgument,
		},
		{
			repo:     testRepo,
			revision: []byte("not-existing-ref"),
			code:     codes.InvalidArgument,
		},
		{
			repo:     testRepo,
			revision: []byte("--output=/meow"),
			code:     codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			req := &gitalypb.ApplyGitattributesRequest{Repository: test.repo, Revision: test.revision}
			_, err := client.ApplyGitattributes(ctx, req)
			testhelper.RequireGrpcError(t, err, test.code)
		})
	}
}

func assertGitattributesApplied(t *testing.T, client gitalypb.RepositoryServiceClient, testRepo *gitalypb.Repository, attributesPath string, revision, expectedContents []byte) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	req := &gitalypb.ApplyGitattributesRequest{Repository: testRepo, Revision: revision}
	c, err := client.ApplyGitattributes(ctx, req)

	assert.NoError(t, err)
	assert.NotNil(t, c)

	contents, err := ioutil.ReadFile(attributesPath)
	if expectedContents == nil {
		if !os.IsNotExist(err) {
			t.Error(err)
		}
	} else {
		if err != nil {
			t.Error(err)
		}

		if info, err := os.Stat(attributesPath); err == nil {
			actualFileMode := info.Mode()
			assert.Equal(t, attributesFileMode, actualFileMode)
		}

		assert.Equal(t, expectedContents, contents)
	}
}
