package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestApplyGitattributesSuccess(t *testing.T) {
	cfg, repo, _, client := setupRepositoryService(t)

	infoPath := filepath.Join(cfg.Storages[0].Path, repo.GetRelativePath(), "info")
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
			assertGitattributesApplied(t, client, repo, attributesPath, test.revision, test.contents)

			// Test when no git attributes file exists
			if err := os.Remove(attributesPath); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			assertGitattributesApplied(t, client, repo, attributesPath, test.revision, test.contents)

			// Test when a git attributes file already exists
			require.NoError(t, os.MkdirAll(infoPath, 0755))
			require.NoError(t, ioutil.WriteFile(attributesPath, []byte("*.docx diff=word"), 0644))
			assertGitattributesApplied(t, client, repo, attributesPath, test.revision, test.contents)
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
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	transactionServer := &testTransactionServer{}
	testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterRepositoryServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetRubyServer(),
			deps.GetLocator(),
			deps.GetTxManager(),
			deps.GetGitCmdFactory(),
			deps.GetCatfileCache(),
		))
	})

	// We're using internal listener in order to route around
	// Praefect in our tests. Otherwise Praefect would replace our
	// carefully crafted transaction and server information.
	logger := testhelper.DiscardTestEntry(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := newMuxedRepositoryClient(t, ctx, cfg, "unix://"+cfg.GitalyInternalSocketPath(),
		backchannel.NewClientHandshaker(logger, func() backchannel.Server {
			srv := grpc.NewServer()
			gitalypb.RegisterRefTransactionServer(srv, transactionServer)
			return srv
		}),
	)

	praefect := txinfo.PraefectServer{
		SocketPath: "unix://" + cfg.GitalyInternalSocketPath(),
		Token:      cfg.Auth.Token,
	}

	for _, tc := range []struct {
		desc        string
		revision    []byte
		voteFn      func(*testing.T, *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error)
		shouldExist bool
		expectedErr error
	}{
		{
			desc:     "successful vote writes gitattributes",
			revision: []byte("e63f41fe459e62e1228fcef60d7189127aeba95a"),
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
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				return nil, errors.New("foobar")
			},
			shouldExist: false,
			expectedErr: status.Error(codes.Unknown, "could not commit gitattributes: vote failed: rpc error: code = Unknown desc = foobar"),
		},
		{
			desc:     "commit without gitattributes performs vote",
			revision: []byte("7efb185dd22fd5c51ef044795d62b7847900c341"),
			voteFn: func(t *testing.T, request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				require.Equal(t, bytes.Repeat([]byte{0x00}, 20), request.ReferenceUpdatesHash)
				return &gitalypb.VoteTransactionResponse{
					State: gitalypb.VoteTransactionResponse_COMMIT,
				}, nil
			},
			shouldExist: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			infoPath := filepath.Join(repoPath, "info")
			require.NoError(t, os.RemoveAll(infoPath))

			ctx, err := txinfo.InjectTransaction(ctx, 1, "primary", true)
			require.NoError(t, err)
			ctx, err = praefect.Inject(ctx)
			require.NoError(t, err)
			ctx = helper.IncomingToOutgoing(ctx)

			transactionServer.vote = func(request *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
				return tc.voteFn(t, request)
			}

			_, err = client.ApplyGitattributes(ctx, &gitalypb.ApplyGitattributesRequest{
				Repository: repo,
				Revision:   tc.revision,
			})
			testassert.GrpcEqualErr(t, tc.expectedErr, err)

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
	_, repo, _, client := setupRepositoryService(t)

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
			repo:     &gitalypb.Repository{StorageName: repo.GetStorageName(), RelativePath: "bar"},
			revision: []byte("master"),
			code:     codes.NotFound,
		},
		{
			repo:     repo,
			revision: []byte(""),
			code:     codes.InvalidArgument,
		},
		{
			repo:     repo,
			revision: []byte("not-existing-ref"),
			code:     codes.InvalidArgument,
		},
		{
			repo:     repo,
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
	t.Helper()

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
