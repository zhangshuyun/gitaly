package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	gitalylog "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	praefectConfig "gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func TestManager_Create(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	cfg := testcfg.Build(t)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc               string
		setup              func(t testing.TB) (*gitalypb.Repository, string)
		createsBundle      bool
		createsCustomHooks bool
		err                error
	}{
		{
			desc: "no hooks",
			setup: func(t testing.TB) (*gitalypb.Repository, string) {
				noHooksRepo, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					RelativePath: "no-hooks",
					Seed:         gittest.SeedGitLabTest,
				})
				return noHooksRepo, repoPath
			},
			createsBundle:      true,
			createsCustomHooks: false,
		},
		{
			desc: "hooks",
			setup: func(t testing.TB) (*gitalypb.Repository, string) {
				hooksRepo, hooksRepoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					RelativePath: "hooks",
					Seed:         gittest.SeedGitLabTest,
				})
				require.NoError(t, os.Mkdir(filepath.Join(hooksRepoPath, "custom_hooks"), os.ModePerm))
				require.NoError(t, os.WriteFile(filepath.Join(hooksRepoPath, "custom_hooks/pre-commit.sample"), []byte("Some hooks"), os.ModePerm))
				return hooksRepo, hooksRepoPath
			},
			createsBundle:      true,
			createsCustomHooks: true,
		},
		{
			desc: "empty repo",
			setup: func(t testing.TB) (*gitalypb.Repository, string) {
				emptyRepo, repoPath := gittest.CreateRepository(ctx, t, cfg)
				return emptyRepo, repoPath
			},
			createsBundle:      false,
			createsCustomHooks: false,
			err:                fmt.Errorf("manager: repository empty: %w", ErrSkipped),
		},
		{
			desc: "nonexistent repo",
			setup: func(t testing.TB) (*gitalypb.Repository, string) {
				emptyRepo, repoPath := gittest.CreateRepository(ctx, t, cfg)
				nonexistentRepo := proto.Clone(emptyRepo).(*gitalypb.Repository)
				nonexistentRepo.RelativePath = "nonexistent"
				return nonexistentRepo, repoPath
			},
			createsBundle:      false,
			createsCustomHooks: false,
			err:                fmt.Errorf("manager: repository empty: %w", ErrSkipped),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repo, repoPath := tc.setup(t)
			path := testhelper.TempDir(t)
			refsPath := filepath.Join(path, repo.RelativePath, backupID, "001.refs")
			bundlePath := filepath.Join(path, repo.RelativePath, backupID, "001.bundle")
			customHooksPath := filepath.Join(path, repo.RelativePath, backupID, "001.custom_hooks.tar")

			pool := client.NewPool()
			defer testhelper.MustClose(t, pool)

			sink := NewFilesystemSink(path)
			locator, err := ResolveLocator("pointer", sink)
			require.NoError(t, err)

			fsBackup := NewManager(sink, locator, pool, backupID)
			err = fsBackup.Create(ctx, &CreateRequest{
				Server:     storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
				Repository: repo,
			})
			if tc.err == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.err, err)
			}

			if tc.createsBundle {
				require.FileExists(t, refsPath)
				require.FileExists(t, bundlePath)

				dirInfo, err := os.Stat(filepath.Dir(bundlePath))
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm(), "expecting restricted directory permissions")

				bundleInfo, err := os.Stat(bundlePath)
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0o600), bundleInfo.Mode().Perm(), "expecting restricted file permissions")

				output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundlePath)
				require.Contains(t, string(output), "The bundle records a complete history")

				expectedRefs := gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--head")
				actualRefs := testhelper.MustReadFile(t, refsPath)
				require.Equal(t, string(expectedRefs), string(actualRefs))
			} else {
				require.NoFileExists(t, bundlePath)
			}

			if tc.createsCustomHooks {
				require.FileExists(t, customHooksPath)
			} else {
				require.NoFileExists(t, customHooksPath)
			}
		})
	}
}

func TestManager_Create_incremental(t *testing.T) {
	t.Parallel()

	const backupID = "abc123"

	cfg := testcfg.Build(t)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc              string
		setup             func(t testing.TB, backupRoot string) (*gitalypb.Repository, string)
		expectedIncrement string
		expectedErr       error
	}{
		{
			desc: "no previous backup",
			setup: func(t testing.TB, backupRoot string) (*gitalypb.Repository, string) {
				repo, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					RelativePath: "repo",
					Seed:         gittest.SeedGitLabTest,
				})
				return repo, repoPath
			},
			expectedIncrement: "001",
		},
		{
			desc: "previous backup, no updates",
			setup: func(t testing.TB, backupRoot string) (*gitalypb.Repository, string) {
				repo, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					RelativePath: "repo",
					Seed:         gittest.SeedGitLabTest,
				})

				backupRepoPath := filepath.Join(backupRoot, repo.RelativePath)
				backupPath := filepath.Join(backupRepoPath, backupID)
				bundlePath := filepath.Join(backupPath, "001.bundle")
				refsPath := filepath.Join(backupPath, "001.refs")

				require.NoError(t, os.MkdirAll(backupPath, os.ModePerm))
				gittest.Exec(t, cfg, "-C", repoPath, "bundle", "create", bundlePath, "--all")

				refs := gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--head")
				require.NoError(t, os.WriteFile(refsPath, refs, os.ModePerm))

				require.NoError(t, os.WriteFile(filepath.Join(backupRepoPath, "LATEST"), []byte(backupID), os.ModePerm))
				require.NoError(t, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("001"), os.ModePerm))

				return repo, repoPath
			},
			expectedErr: fmt.Errorf("manager: write bundle: %w", fmt.Errorf("*backup.FilesystemSink write: %w: no changes to bundle", ErrSkipped)),
		},
		{
			desc: "previous backup, updates",
			setup: func(t testing.TB, backupRoot string) (*gitalypb.Repository, string) {
				repo, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					RelativePath: "repo",
					Seed:         gittest.SeedGitLabTest,
				})

				backupRepoPath := filepath.Join(backupRoot, repo.RelativePath)
				backupPath := filepath.Join(backupRepoPath, backupID)
				bundlePath := filepath.Join(backupPath, "001.bundle")
				refsPath := filepath.Join(backupPath, "001.refs")

				require.NoError(t, os.MkdirAll(backupPath, os.ModePerm))
				gittest.Exec(t, cfg, "-C", repoPath, "bundle", "create", bundlePath, "--all")

				refs := gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--head")
				require.NoError(t, os.WriteFile(refsPath, refs, os.ModePerm))

				require.NoError(t, os.WriteFile(filepath.Join(backupRepoPath, "LATEST"), []byte(backupID), os.ModePerm))
				require.NoError(t, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("001"), os.ModePerm))

				gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

				return repo, repoPath
			},
			expectedIncrement: "002",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			path := testhelper.TempDir(t)
			repo, repoPath := tc.setup(t, path)

			refsPath := filepath.Join(path, repo.RelativePath, backupID, tc.expectedIncrement+".refs")
			bundlePath := filepath.Join(path, repo.RelativePath, backupID, tc.expectedIncrement+".bundle")

			pool := client.NewPool()
			defer testhelper.MustClose(t, pool)

			sink := NewFilesystemSink(path)
			locator, err := ResolveLocator("pointer", sink)
			require.NoError(t, err)

			fsBackup := NewManager(sink, locator, pool, backupID)
			err = fsBackup.Create(ctx, &CreateRequest{
				Server:      storage.ServerInfo{Address: cfg.SocketPath, Token: cfg.Auth.Token},
				Repository:  repo,
				Incremental: true,
			})
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, tc.expectedErr, err)
				return
			}

			require.FileExists(t, refsPath)
			require.FileExists(t, bundlePath)

			expectedRefs := gittest.Exec(t, cfg, "-C", repoPath, "show-ref", "--head")
			actualRefs := testhelper.MustReadFile(t, refsPath)
			require.Equal(t, string(expectedRefs), string(actualRefs))
		})
	}
}

func TestManager_Restore(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	testcfg.BuildGitalyHooks(t, cfg)

	cfg.SocketPath = testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)
	ctx := testhelper.Context(t)

	testManagerRestore(t, ctx, cfg, cfg.SocketPath)
}

func TestManager_Restore_praefect(t *testing.T) {
	t.Parallel()

	gitalyCfg := testcfg.Build(t, testcfg.WithStorages("gitaly-1"))

	testcfg.BuildGitalyHooks(t, gitalyCfg)

	gitalyCfg.SocketPath = testserver.RunGitalyServer(t, gitalyCfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	conf := praefectConfig.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*praefectConfig.VirtualStorage{
			{
				Name: "default",
				Nodes: []*praefectConfig.Node{
					{Storage: gitalyCfg.Storages[0].Name, Address: gitalyCfg.SocketPath},
				},
			},
		},
		DB: testdb.GetConfig(t, testdb.New(t).Name),
		Failover: praefectConfig.Failover{
			Enabled:          true,
			ElectionStrategy: praefectConfig.ElectionStrategyPerRepository,
		},
		Replication: praefectConfig.DefaultReplicationConfig(),
		Logging: gitalylog.Config{
			Format: "json",
			Level:  "panic",
		},
	}
	ctx := testhelper.Context(t)

	testManagerRestore(t, ctx, gitalyCfg, testserver.StartPraefect(t, conf).Address())
}

func testManagerRestore(t *testing.T, ctx context.Context, cfg config.Cfg, gitalyAddr string) {
	cc, err := client.Dial(gitalyAddr, nil)
	require.NoError(t, err)
	defer testhelper.MustClose(t, cc)

	repoClient := gitalypb.NewRepositoryServiceClient(cc)

	createRepo := func(t testing.TB) *gitalypb.Repository {
		t.Helper()

		repo := &gitalypb.Repository{
			StorageName:  "default",
			RelativePath: gittest.NewRepositoryName(t, false),
		}

		_, err := repoClient.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repo})
		require.NoError(t, err)

		// The repository might be created through Praefect and the tests reach into the repository directly
		// on the filesystem. To ensure the test accesses correct directory, we need to rewrite the relative
		// path if the repository creation went through Praefect.
		repo.RelativePath = gittest.GetReplicaPath(ctx, t, cfg, repo)

		return repo
	}

	path := testhelper.TempDir(t)
	testRepoChecksum := gittest.ChecksumTestRepo(t, cfg, "gitlab-test.git")

	for _, tc := range []struct {
		desc          string
		locators      []string
		setup         func(t testing.TB) (*gitalypb.Repository, *git.Checksum)
		alwaysCreate  bool
		expectExists  bool
		expectedPaths []string
		expectedErrAs error
	}{
		{
			desc:     "existing repo, without hooks",
			locators: []string{"legacy", "pointer"},
			setup: func(t testing.TB) (*gitalypb.Repository, *git.Checksum) {
				repo := createRepo(t)
				require.NoError(t, os.MkdirAll(filepath.Join(path, repo.RelativePath), os.ModePerm))
				bundlePath := filepath.Join(path, repo.RelativePath+".bundle")
				gittest.BundleTestRepo(t, cfg, "gitlab-test.git", bundlePath)

				return repo, testRepoChecksum
			},
			expectExists: true,
		},
		{
			desc:     "existing repo, with hooks",
			locators: []string{"legacy", "pointer"},
			setup: func(t testing.TB) (*gitalypb.Repository, *git.Checksum) {
				repo := createRepo(t)
				bundlePath := filepath.Join(path, repo.RelativePath+".bundle")
				customHooksPath := filepath.Join(path, repo.RelativePath, "custom_hooks.tar")
				require.NoError(t, os.MkdirAll(filepath.Join(path, repo.RelativePath), os.ModePerm))
				gittest.BundleTestRepo(t, cfg, "gitlab-test.git", bundlePath)
				testhelper.CopyFile(t, "../gitaly/service/repository/testdata/custom_hooks.tar", customHooksPath)

				return repo, testRepoChecksum
			},
			expectedPaths: []string{
				"custom_hooks/pre-commit.sample",
				"custom_hooks/prepare-commit-msg.sample",
				"custom_hooks/pre-push.sample",
			},
			expectExists: true,
		},
		{
			desc:     "missing bundle",
			locators: []string{"legacy", "pointer"},
			setup: func(t testing.TB) (*gitalypb.Repository, *git.Checksum) {
				repo := createRepo(t)
				return repo, nil
			},
			expectedErrAs: ErrSkipped,
		},
		{
			desc:     "missing bundle, always create",
			locators: []string{"legacy", "pointer"},
			setup: func(t testing.TB) (*gitalypb.Repository, *git.Checksum) {
				repo := createRepo(t)
				return repo, new(git.Checksum)
			},
			alwaysCreate: true,
			expectExists: true,
		},
		{
			desc:     "nonexistent repo",
			locators: []string{"legacy", "pointer"},
			setup: func(t testing.TB) (*gitalypb.Repository, *git.Checksum) {
				repo := &gitalypb.Repository{
					StorageName:  "default",
					RelativePath: gittest.NewRepositoryName(t, false),
				}

				require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(path, repo.RelativePath)), os.ModePerm))
				bundlePath := filepath.Join(path, repo.RelativePath+".bundle")
				gittest.BundleTestRepo(t, cfg, "gitlab-test.git", bundlePath)

				return repo, testRepoChecksum
			},
			expectExists: true,
		},
		{
			desc:     "single incremental",
			locators: []string{"pointer"},
			setup: func(t testing.TB) (*gitalypb.Repository, *git.Checksum) {
				const backupID = "abc123"
				repo := createRepo(t)
				repoBackupPath := filepath.Join(path, repo.RelativePath)
				backupPath := filepath.Join(repoBackupPath, backupID)
				require.NoError(t, os.MkdirAll(backupPath, os.ModePerm))
				require.NoError(t, os.WriteFile(filepath.Join(repoBackupPath, "LATEST"), []byte(backupID), os.ModePerm))
				require.NoError(t, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("001"), os.ModePerm))
				bundlePath := filepath.Join(backupPath, "001.bundle")
				gittest.BundleTestRepo(t, cfg, "gitlab-test.git", bundlePath)

				return repo, testRepoChecksum
			},
			expectExists: true,
		},
		{
			desc:     "many incrementals",
			locators: []string{"pointer"},
			setup: func(t testing.TB) (*gitalypb.Repository, *git.Checksum) {
				const backupID = "abc123"

				expected := createRepo(t)
				expectedRepoPath := filepath.Join(cfg.Storages[0].Path, expected.RelativePath)

				repo := createRepo(t)
				repoBackupPath := filepath.Join(path, repo.RelativePath)
				backupPath := filepath.Join(repoBackupPath, backupID)
				require.NoError(t, os.MkdirAll(backupPath, os.ModePerm))
				require.NoError(t, os.WriteFile(filepath.Join(repoBackupPath, "LATEST"), []byte(backupID), os.ModePerm))
				require.NoError(t, os.WriteFile(filepath.Join(backupPath, "LATEST"), []byte("002"), os.ModePerm))

				root := gittest.WriteCommit(t, cfg, expectedRepoPath,
					gittest.WithBranch("master"),
					gittest.WithParents(),
				)
				master1 := gittest.WriteCommit(t, cfg, expectedRepoPath,
					gittest.WithBranch("master"),
					gittest.WithParents(root),
				)
				other := gittest.WriteCommit(t, cfg, expectedRepoPath,
					gittest.WithBranch("other"),
					gittest.WithParents(root),
				)
				gittest.Exec(t, cfg, "-C", expectedRepoPath, "symbolic-ref", "HEAD", "refs/heads/master")
				bundlePath1 := filepath.Join(backupPath, "001.bundle")
				gittest.Exec(t, cfg, "-C", expectedRepoPath, "bundle", "create", bundlePath1,
					"HEAD",
					"refs/heads/master",
					"refs/heads/other",
				)

				master2 := gittest.WriteCommit(t, cfg, expectedRepoPath,
					gittest.WithBranch("master"),
					gittest.WithParents(master1),
				)
				bundlePath2 := filepath.Join(backupPath, "002.bundle")
				gittest.Exec(t, cfg, "-C", expectedRepoPath, "bundle", "create", bundlePath2,
					"HEAD",
					"^"+master1.String(),
					"^"+other.String(),
					"refs/heads/master",
					"refs/heads/other",
				)

				checksum := new(git.Checksum)
				checksum.Add(git.NewReference("HEAD", master2.String()))
				checksum.Add(git.NewReference("refs/heads/master", master2.String()))
				checksum.Add(git.NewReference("refs/heads/other", other.String()))

				return repo, checksum
			},
			expectExists: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.GreaterOrEqual(t, len(tc.locators), 1, "each test case must specify a locator")

			for _, locatorName := range tc.locators {
				t.Run(locatorName, func(t *testing.T) {
					repo, expectedChecksum := tc.setup(t)

					pool := client.NewPool()
					defer testhelper.MustClose(t, pool)

					sink := NewFilesystemSink(path)
					locator, err := ResolveLocator(locatorName, sink)
					require.NoError(t, err)

					fsBackup := NewManager(sink, locator, pool, "unused-backup-id")
					err = fsBackup.Restore(ctx, &RestoreRequest{
						Server:       storage.ServerInfo{Address: gitalyAddr, Token: cfg.Auth.Token},
						Repository:   repo,
						AlwaysCreate: tc.alwaysCreate,
					})
					if tc.expectedErrAs != nil {
						require.ErrorAs(t, err, &tc.expectedErrAs)
					} else {
						require.NoError(t, err)
					}

					exists, err := repoClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{
						Repository: repo,
					})
					require.NoError(t, err)
					require.Equal(t, tc.expectExists, exists.Exists, "repository exists")

					if expectedChecksum != nil {
						checksum, err := repoClient.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
							Repository: repo,
						})
						require.NoError(t, err)

						require.Equal(t, expectedChecksum.String(), checksum.GetChecksum())
					}

					if len(tc.expectedPaths) > 0 {
						// Restore has to use the rewritten path as the relative path due to the test creating
						// the repository through Praefect. In order to get to the correct disk paths, we need
						// to get the replica path of the rewritten repository.
						repoPath := filepath.Join(cfg.Storages[0].Path, gittest.GetReplicaPath(ctx, t, cfg, repo))
						for _, p := range tc.expectedPaths {
							require.FileExists(t, filepath.Join(repoPath, p))
						}
					}
				})
			}
		})
	}
}

func TestResolveSink(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	isStorageServiceSink := func(expErrMsg string) func(t *testing.T, sink Sink) {
		return func(t *testing.T, sink Sink) {
			t.Helper()
			sssink, ok := sink.(*StorageServiceSink)
			require.True(t, ok)
			_, err := sssink.bucket.List(nil).Next(ctx)
			ierr, ok := err.(interface{ Unwrap() error })
			require.True(t, ok)
			terr := ierr.Unwrap()
			require.Contains(t, terr.Error(), expErrMsg)
		}
	}

	tmpDir := testhelper.TempDir(t)
	gsCreds := filepath.Join(tmpDir, "gs.creds")
	require.NoError(t, os.WriteFile(gsCreds, []byte(`
{
  "type": "service_account",
  "project_id": "hostfactory-179005",
  "private_key_id": "6253b144ccd94f50ce1224a73ffc48bda256d0a7",
  "private_key": "-----BEGIN PRIVATE KEY-----\nXXXX<KEY CONTENT OMMIT HERR> \n-----END PRIVATE KEY-----\n",
  "client_email": "303721356529-compute@developer.gserviceaccount.com",
  "client_id": "116595416948414952474",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/303724477529-compute%40developer.gserviceaccount.com"
}`), 0o655))

	for _, tc := range []struct {
		desc   string
		envs   map[string]string
		path   string
		verify func(t *testing.T, sink Sink)
		errMsg string
	}{
		{
			desc: "AWS S3",
			envs: map[string]string{
				"AWS_ACCESS_KEY_ID":     "test",
				"AWS_SECRET_ACCESS_KEY": "test",
				"AWS_REGION":            "us-east-1",
			},
			path:   "s3://bucket",
			verify: isStorageServiceSink("The AWS Access Key Id you provided does not exist in our records."),
		},
		{
			desc: "Google Cloud Storage",
			envs: map[string]string{
				"GOOGLE_APPLICATION_CREDENTIALS": gsCreds,
			},
			path:   "blob+gs://bucket",
			verify: isStorageServiceSink("storage.googleapis.com"),
		},
		{
			desc: "Azure Cloud File Storage",
			envs: map[string]string{
				"AZURE_STORAGE_ACCOUNT":   "test",
				"AZURE_STORAGE_KEY":       "test",
				"AZURE_STORAGE_SAS_TOKEN": "test",
			},
			path:   "blob+bucket+azblob://bucket",
			verify: isStorageServiceSink("https://test.blob.core.windows.net"),
		},
		{
			desc: "Filesystem",
			path: "/some/path",
			verify: func(t *testing.T, sink Sink) {
				require.IsType(t, &FilesystemSink{}, sink)
			},
		},
		{
			desc:   "undefined",
			path:   "some:invalid:path\x00",
			errMsg: `parse "some:invalid:path\x00": net/url: invalid control character in URL`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			for k, v := range tc.envs {
				testhelper.ModifyEnvironment(t, k, v)
			}
			sink, err := ResolveSink(ctx, tc.path)
			if tc.errMsg != "" {
				require.EqualError(t, err, tc.errMsg)
				return
			}
			tc.verify(t, sink)
		})
	}
}

func TestResolveLocator(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		layout      string
		expectedErr string
	}{
		{layout: "legacy"},
		{layout: "pointer"},
		{
			layout:      "unknown",
			expectedErr: "unknown layout: \"unknown\"",
		},
	} {
		t.Run(tc.layout, func(t *testing.T) {
			l, err := ResolveLocator(tc.layout, nil)

			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr)
				return
			}

			require.NotNil(t, l)
		})
	}
}
