package repository

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestApplyGitattributesSuccess(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	infoPath := path.Join(testhelper.GitlabTestStoragePath(),
		testRepo.GetRelativePath(), "info")
	attributesPath := path.Join(infoPath, "attributes")

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

func TestApplyGitattributesFailure(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
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
