package repository

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func testSuccessfulFindLicenseRequest(t *testing.T, cfg config.Cfg, client gitalypb.RepositoryServiceClient, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets(featureflag.GoFindLicense).Run(t, func(t *testing.T, ctx context.Context) {
		for _, tc := range []struct {
			desc                  string
			nonExistentRepository bool
			files                 map[string]string
			expectedLicense       string
			errorContains         string
		}{
			{
				desc:                  "repository does not exist",
				nonExistentRepository: true,
				errorContains:         "rpc error: code = NotFound desc = GetRepoPath: not a git repository",
			},
			{
				desc: "empty if no license file in repo",
				files: map[string]string{
					"README.md": "readme content",
				},
				expectedLicense: "",
			},
			{
				desc: "high confidence mit result and less confident mit-0 result",
				files: map[string]string{
					"LICENSE": `MIT License

Copyright (c) [year] [fullname]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.`,
				},
				expectedLicense: "mit",
			},
			{
				desc: "unknown license",
				files: map[string]string{
					"LICENSE.md": "this doesn't match any known license",
				},
				expectedLicense: "other",
			},
		} {
			t.Run(tc.desc, func(t *testing.T) {
				repo, repoPath := gittest.CreateRepository(ctx, t, cfg)

				var treeEntries []gittest.TreeEntry
				for file, content := range tc.files {
					treeEntries = append(treeEntries, gittest.TreeEntry{
						Mode:    "100644",
						Path:    file,
						Content: content,
					})
				}

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("main"), gittest.WithTreeEntries(treeEntries...), gittest.WithParents())

				if tc.nonExistentRepository {
					require.NoError(t, os.RemoveAll(repoPath))
				}

				resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: repo})
				if tc.errorContains != "" {
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.errorContains)
					return
				}

				require.NoError(t, err)
				testhelper.ProtoEqual(t, &gitalypb.FindLicenseResponse{
					LicenseShortName: tc.expectedLicense,
				}, resp)
			})
		}
	})
}

func testFindLicenseRequestEmptyRepo(t *testing.T, cfg config.Cfg, client gitalypb.RepositoryServiceClient, rubySrv *rubyserver.Server) {
	testhelper.NewFeatureSets(featureflag.GoFindLicense).Run(t, func(t *testing.T, ctx context.Context) {
		repo, repoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])
		require.NoError(t, os.RemoveAll(repoPath))

		_, err := client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repo})
		require.NoError(t, err)

		resp, err := client.FindLicense(ctx, &gitalypb.FindLicenseRequest{Repository: repo})
		require.NoError(t, err)

		require.Empty(t, resp.GetLicenseShortName())
	})
}
