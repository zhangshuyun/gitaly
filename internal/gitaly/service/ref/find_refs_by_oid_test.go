package ref

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestFindRefsByOID_successful(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repo, repoPath, client := setupRefService(t)

	oid := gittest.WriteCommit(t, cfg, repoPath)

	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/branch-1", string(oid))
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/branch-2", string(oid))
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/branch-3", string(oid))
	gittest.Exec(t, cfg, "-C", repoPath, "tag", "v100.0.0", string(oid))
	gittest.Exec(t, cfg, "-C", repoPath, "tag", "v100.1.0", string(oid))
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/branch-4", string(oid))
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/branch-5", string(oid))

	t.Run("tags come first", func(t *testing.T) {
		resp, err := client.FindRefsByOID(ctx, &gitalypb.FindRefsByOIDRequest{
			Repository: repo,
			Oid:        string(oid),
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"refs/heads/branch-1",
			"refs/heads/branch-2",
			"refs/heads/branch-3",
			"refs/heads/branch-4",
			"refs/heads/branch-5",
			"refs/tags/v100.0.0",
			"refs/tags/v100.1.0",
		}, resp.GetRefs())
	})

	t.Run("limit the response", func(t *testing.T) {
		resp, err := client.FindRefsByOID(ctx, &gitalypb.FindRefsByOIDRequest{
			Repository: repo,
			Oid:        string(oid),
			Limit:      3,
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"refs/heads/branch-1",
			"refs/heads/branch-2",
			"refs/heads/branch-3",
		}, resp.GetRefs())
	})

	t.Run("excludes other tags", func(t *testing.T) {
		anotherSha := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("hello! this is another commit"))
		gittest.Exec(t, cfg, "-C", repoPath, "tag", "v101.1.0", string(anotherSha))

		resp, err := client.FindRefsByOID(ctx, &gitalypb.FindRefsByOIDRequest{
			Repository: repo,
			Oid:        string(oid),
		})
		assert.NoError(t, err)
		assert.NotContains(t, resp.GetRefs(), "refs/tags/v101.1.0")
	})

	t.Run("oid prefix", func(t *testing.T) {
		resp, err := client.FindRefsByOID(ctx, &gitalypb.FindRefsByOIDRequest{
			Repository: repo,
			Oid:        string(oid)[:6],
			Limit:      1,
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"refs/heads/branch-1",
		}, resp.GetRefs())
	})

	t.Run("sort field", func(t *testing.T) {
		resp, err := client.FindRefsByOID(ctx, &gitalypb.FindRefsByOIDRequest{
			Repository:  repo,
			Oid:         string(oid),
			RefPatterns: []string{"refs/heads/"},
			Limit:       3,
			SortField:   "-refname",
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"refs/heads/branch-5",
			"refs/heads/branch-4",
			"refs/heads/branch-3",
		}, resp.GetRefs())
	})

	t.Run("ref patterns", func(t *testing.T) {
		resp, err := client.FindRefsByOID(ctx, &gitalypb.FindRefsByOIDRequest{
			Repository:  repo,
			Oid:         string(oid),
			RefPatterns: []string{"refs/tags/"},
		})
		assert.NoError(t, err)
		assert.Equal(t, []string{
			"refs/tags/v100.0.0",
			"refs/tags/v100.1.0",
		}, resp.GetRefs())
	})
}

func TestFindRefsByOID_failure(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, client := setupRefServiceWithoutRepo(t)

	testCases := []struct {
		desc  string
		setup func(t *testing.T) (*gitalypb.FindRefsByOIDRequest, error)
	}{
		{
			desc: "no ref exists for OID",
			setup: func(t *testing.T) (*gitalypb.FindRefsByOIDRequest, error) {
				repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				oid := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("no ref exists for OID"))

				return &gitalypb.FindRefsByOIDRequest{
					Repository: repo,
					Oid:        oid.String(),
				}, nil
			},
		},
		{
			desc: "repository is corrupted",
			setup: func(t *testing.T) (*gitalypb.FindRefsByOIDRequest, error) {
				repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				oid := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("no ref exists for OID"))
				gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/corrupted-repo-branch", oid.String())

				require.NoError(t, os.RemoveAll(filepath.Join(repoPath, "objects")))

				return &gitalypb.FindRefsByOIDRequest{
					Repository: repo,
					Oid:        oid.String(),
				}, helper.ErrNotFoundf("GetRepoPath: not a git repository: %q", repoPath)
			},
		},
		{
			desc: "repository is missing",
			setup: func(t *testing.T) (*gitalypb.FindRefsByOIDRequest, error) {
				repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				oid := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("repository is missing"))
				require.NoError(t, os.RemoveAll(repoPath))

				return &gitalypb.FindRefsByOIDRequest{
					Repository: repo,
					Oid:        oid.String(),
				}, helper.ErrNotFoundf("GetRepoPath: not a git repository: %q", repoPath)
			},
		},
		{
			desc: "oid is not a commit",
			setup: func(t *testing.T) (*gitalypb.FindRefsByOIDRequest, error) {
				repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				oid := gittest.WriteBlob(t, cfg, repoPath, []byte("the blob"))

				return &gitalypb.FindRefsByOIDRequest{
					Repository: repo,
					Oid:        oid.String(),
				}, nil
			},
		},
		{
			desc: "oid prefix too short",
			setup: func(t *testing.T) (*gitalypb.FindRefsByOIDRequest, error) {
				repo, repoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])
				oid := gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage("oid prefix too short"))
				gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/short-oid", oid.String())

				return &gitalypb.FindRefsByOIDRequest{
					Repository: repo,
					Oid:        oid.String()[:2],
				}, helper.ErrInvalidArgumentf("for-each-ref pipeline command: exit status 129")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request, expectedErr := tc.setup(t)

			response, err := client.FindRefsByOID(ctx, request)
			require.Empty(t, response.GetRefs())
			testhelper.GrpcEqualErr(t, expectedErr, err)
		})
	}
}

func TestFindRefsByOID_validation(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, repo, _, client := setupRefService(t)

	testCases := map[string]struct {
		req          *gitalypb.FindRefsByOIDRequest
		expectedMsg  string
		expectedCode codes.Code
	}{
		"no repository": {
			req: &gitalypb.FindRefsByOIDRequest{
				Repository: nil,
				Oid:        "abcdefg",
			},
			expectedMsg:  "empty Repository",
			expectedCode: codes.InvalidArgument,
		},
		"no oid": {
			req: &gitalypb.FindRefsByOIDRequest{
				Repository: repo,
				Oid:        "",
			},
			expectedMsg:  "empty Oid",
			expectedCode: codes.InvalidArgument,
		},
	}

	for tn, tc := range testCases {
		t.Run(tn, func(t *testing.T) {
			_, err := client.FindRefsByOID(ctx, tc.req)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedMsg)
			testhelper.RequireGrpcCode(t, err, tc.expectedCode)
		})
	}
}
