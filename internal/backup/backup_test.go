package backup

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

func TestManager_Create(t *testing.T) {
	cfg := testcfg.Build(t)

	gitalyAddr := testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	path := testhelper.TempDir(t)

	hooksRepo, hooksRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0], gittest.CloneRepoOpts{
		RelativePath: "hooks",
	})
	require.NoError(t, os.Mkdir(filepath.Join(hooksRepoPath, "custom_hooks"), os.ModePerm))
	require.NoError(t, ioutil.WriteFile(filepath.Join(hooksRepoPath, "custom_hooks/pre-commit.sample"), []byte("Some hooks"), os.ModePerm))

	noHooksRepo, _ := gittest.CloneRepo(t, cfg, cfg.Storages[0], gittest.CloneRepoOpts{
		RelativePath: "no-hooks",
	})
	emptyRepo, _ := gittest.InitRepo(t, cfg, cfg.Storages[0])
	nonexistentRepo := proto.Clone(emptyRepo).(*gitalypb.Repository)
	nonexistentRepo.RelativePath = "nonexistent"

	for _, tc := range []struct {
		desc               string
		repo               *gitalypb.Repository
		createsBundle      bool
		createsCustomHooks bool
		err                error
	}{
		{
			desc:               "no hooks",
			repo:               noHooksRepo,
			createsBundle:      true,
			createsCustomHooks: false,
		},
		{
			desc:               "hooks",
			repo:               hooksRepo,
			createsBundle:      true,
			createsCustomHooks: true,
		},
		{
			desc:               "empty repo",
			repo:               emptyRepo,
			createsBundle:      false,
			createsCustomHooks: false,
			err:                ErrSkipped,
		},
		{
			desc:               "nonexistent repo",
			repo:               nonexistentRepo,
			createsBundle:      false,
			createsCustomHooks: false,
			err:                ErrSkipped,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoPath := filepath.Join(cfg.Storages[0].Path, tc.repo.RelativePath)
			refsPath := filepath.Join(path, tc.repo.RelativePath+".refs")
			bundlePath := filepath.Join(path, tc.repo.RelativePath+".bundle")
			customHooksPath := filepath.Join(path, tc.repo.RelativePath, "custom_hooks.tar")

			ctx, cancel := testhelper.Context()
			defer cancel()

			fsBackup := NewManager(NewFilesystemSink(path))
			err := fsBackup.Create(ctx, &CreateRequest{
				Server:     storage.ServerInfo{Address: gitalyAddr, Token: cfg.Auth.Token},
				Repository: tc.repo,
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
				require.Equal(t, os.FileMode(0700), dirInfo.Mode().Perm(), "expecting restricted directory permissions")

				bundleInfo, err := os.Stat(bundlePath)
				require.NoError(t, err)
				require.Equal(t, os.FileMode(0600), bundleInfo.Mode().Perm(), "expecting restricted file permissions")

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

func TestManager_Restore(t *testing.T) {
	cfg := testcfg.Build(t)
	testhelper.BuildGitalyHooks(t, cfg)

	gitalyAddr := testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll)

	path := testhelper.TempDir(t)

	existingRepo, existRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0], gittest.CloneRepoOpts{
		RelativePath: "existing_repo",
	})
	existingRepoPath := filepath.Join(path, existingRepo.RelativePath)
	existingRepoBundlePath := existingRepoPath + ".bundle"
	existingRepoCustomHooksPath := filepath.Join(existingRepoPath, "custom_hooks.tar")
	require.NoError(t, os.MkdirAll(existingRepoPath, os.ModePerm))

	gittest.Exec(t, cfg, "-C", existRepoPath, "bundle", "create", existingRepoBundlePath, "--all")
	testhelper.CopyFile(t, "../gitaly/service/repository/testdata/custom_hooks.tar", existingRepoCustomHooksPath)

	newRepo := gittest.InitRepoDir(t, cfg.Storages[0].Path, "new_repo")
	newRepoBundlePath := filepath.Join(path, newRepo.RelativePath+".bundle")
	testhelper.CopyFile(t, existingRepoBundlePath, newRepoBundlePath)

	missingBundleRepo := gittest.InitRepoDir(t, cfg.Storages[0].Path, "missing_bundle")
	missingBundleRepoAlwaysCreate := gittest.InitRepoDir(t, cfg.Storages[0].Path, "missing_bundle_always_create")

	for _, tc := range []struct {
		desc          string
		repo          *gitalypb.Repository
		alwaysCreate  bool
		expectedPaths []string
		expectedErrAs error
		expectVerify  bool
	}{
		{
			desc:         "new repo, without hooks",
			repo:         newRepo,
			expectVerify: true,
		},
		{
			desc: "existing repo, with hooks",
			repo: existingRepo,
			expectedPaths: []string{
				"custom_hooks/pre-commit.sample",
				"custom_hooks/prepare-commit-msg.sample",
				"custom_hooks/pre-push.sample",
			},
			expectVerify: true,
		},
		{
			desc:          "missing bundle",
			repo:          missingBundleRepo,
			expectedErrAs: ErrSkipped,
		},
		{
			desc:         "missing bundle, always create",
			repo:         missingBundleRepoAlwaysCreate,
			alwaysCreate: true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repoPath := filepath.Join(cfg.Storages[0].Path, tc.repo.RelativePath)
			bundlePath := filepath.Join(path, tc.repo.RelativePath+".bundle")

			ctx, cancel := testhelper.Context()
			defer cancel()

			fsBackup := NewManager(NewFilesystemSink(path))
			err := fsBackup.Restore(ctx, &RestoreRequest{
				Server:       storage.ServerInfo{Address: gitalyAddr, Token: cfg.Auth.Token},
				Repository:   tc.repo,
				AlwaysCreate: tc.alwaysCreate,
			})
			if tc.expectedErrAs != nil {
				require.True(t, errors.Is(err, tc.expectedErrAs), err.Error())
			} else {
				require.NoError(t, err)
			}

			if tc.expectVerify {
				output := gittest.Exec(t, cfg, "-C", repoPath, "bundle", "verify", bundlePath)
				require.Contains(t, string(output), "The bundle records a complete history")
			}

			for _, p := range tc.expectedPaths {
				require.FileExists(t, filepath.Join(repoPath, p))
			}
		})
	}
}

func TestResolveSink(t *testing.T) {
	isStorageServiceSink := func(expErrMsg string) func(t *testing.T, sink Sink) {
		return func(t *testing.T, sink Sink) {
			t.Helper()
			sssink, ok := sink.(*StorageServiceSink)
			require.True(t, ok)
			_, err := sssink.bucket.List(nil).Next(context.TODO())
			ierr, ok := err.(interface{ Unwrap() error })
			require.True(t, ok)
			terr := ierr.Unwrap()
			require.Contains(t, terr.Error(), expErrMsg)
		}
	}

	tmpDir := testhelper.TempDir(t)
	gsCreds := filepath.Join(tmpDir, "gs.creds")
	require.NoError(t, ioutil.WriteFile(gsCreds, []byte(`
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
}`), 0655))

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
				t.Cleanup(testhelper.ModifyEnvironment(t, k, v))
			}
			sink, err := ResolveSink(context.TODO(), tc.path)
			if tc.errMsg != "" {
				require.EqualError(t, err, tc.errMsg)
				return
			}
			tc.verify(t, sink)
		})
	}
}
