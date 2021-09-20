package updateref

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/quarantine"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestUpdaterWithHooks_UpdateReference_invalidParameters(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	user := &gitalypb.User{
		GlId:       "1234",
		GlUsername: "Username",
		Name:       []byte("Name"),
		Email:      []byte("mail@example.com"),
	}

	revA, revB := git.ObjectID(strings.Repeat("a", 40)), git.ObjectID(strings.Repeat("b", 40))

	updater := NewUpdaterWithHooks(cfg, &hook.MockManager{}, nil, nil)

	testCases := []struct {
		desc           string
		ref            git.ReferenceName
		newRev, oldRev git.ObjectID
		expectedErr    string
	}{
		{
			desc:        "missing reference",
			oldRev:      revA,
			newRev:      revB,
			expectedErr: "got no reference",
		},
		{
			desc:        "missing old rev",
			ref:         "refs/heads/master",
			newRev:      revB,
			expectedErr: "got invalid old value",
		},
		{
			desc:        "missing new rev",
			ref:         "refs/heads/master",
			oldRev:      revB,
			expectedErr: "got invalid new value",
		},
		{
			desc:        "invalid old rev",
			ref:         "refs/heads/master",
			newRev:      revA,
			oldRev:      "foobar",
			expectedErr: "got invalid old value",
		},
		{
			desc:        "invalid new rev",
			ref:         "refs/heads/master",
			newRev:      "foobar",
			oldRev:      revB,
			expectedErr: "got invalid new value",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := updater.UpdateReference(ctx, repo, user, nil, tc.ref, tc.newRev, tc.oldRev)
			require.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

func TestUpdaterWithHooks_UpdateReference(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	// We need to set up a separate "real" hook service here, as it will be used in
	// git-update-ref(1) spawned by `updateRefWithHooks()`
	testserver.RunGitalyServer(t, cfg, nil, func(srv *grpc.Server, deps *service.Dependencies) {
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory(), deps.GetPackObjectsCache()))
	})

	user := &gitalypb.User{
		GlId:       "1234",
		GlUsername: "Username",
		Name:       []byte("Name"),
		Email:      []byte("mail@example.com"),
	}

	oldRev := "1e292f8fedd741b75372e19097c76d327140c312"

	payload, err := git.NewHooksPayload(cfg, repo, nil, &git.ReceiveHooksPayload{
		UserID:   "1234",
		Username: "Username",
		Protocol: "web",
	}, git.ReceivePackHooks, featureflag.RawFromContext(ctx)).Env()
	require.NoError(t, err)

	expectedEnv := []string{
		payload,
	}

	referenceTransactionCalls := 0
	testCases := []struct {
		desc                 string
		preReceive           func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error
		postReceive          func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error
		update               func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error
		referenceTransaction func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error
		expectedErr          string
		expectedRefDeletion  bool
	}{
		{
			desc: "successful update",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				changes, err := io.ReadAll(stdin)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%s %s refs/heads/master\n", oldRev, git.ZeroOID.String()), string(changes))
				require.Empty(t, pushOptions)
				require.Equal(t, env, expectedEnv)
				return nil
			},
			update: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
				require.Equal(t, "refs/heads/master", ref)
				require.Equal(t, oldRev, oldValue)
				require.Equal(t, newValue, git.ZeroOID.String())
				require.Equal(t, env, expectedEnv)
				return nil
			},
			postReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				changes, err := io.ReadAll(stdin)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%s %s refs/heads/master\n", oldRev, git.ZeroOID.String()), string(changes))
				require.Equal(t, env, expectedEnv)
				require.Empty(t, pushOptions)
				return nil
			},
			referenceTransaction: func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
				changes, err := io.ReadAll(stdin)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%s %s refs/heads/master\n", oldRev, git.ZeroOID.String()), string(changes))

				require.Less(t, referenceTransactionCalls, 2)
				if referenceTransactionCalls == 0 {
					require.Equal(t, state, hook.ReferenceTransactionPrepared)
				} else {
					require.Equal(t, state, hook.ReferenceTransactionCommitted)
				}
				referenceTransactionCalls++

				require.Equal(t, env, expectedEnv)
				return nil
			},
			expectedRefDeletion: true,
		},
		{
			desc: "prereceive error",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				_, err := io.Copy(stderr, strings.NewReader("prereceive failure"))
				require.NoError(t, err)
				return errors.New("ignored")
			},
			expectedErr: "prereceive failure",
		},
		{
			desc: "prereceive error from GitLab API response",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				return hook.NotAllowedError{Message: "GitLab: file is locked"}
			},
			expectedErr: "GitLab: file is locked",
		},
		{
			desc: "update error",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				return nil
			},
			update: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
				_, err := io.Copy(stderr, strings.NewReader("update failure"))
				require.NoError(t, err)
				return errors.New("ignored")
			},
			expectedErr: "update failure",
		},
		{
			desc: "reference-transaction error",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				return nil
			},
			update: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
				return nil
			},
			referenceTransaction: func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
				// The reference-transaction hook doesn't execute any custom hooks,
				// which is why it currently doesn't have any stdout/stderr.
				// Instead, errors are directly returned.
				return errors.New("reference-transaction failure")
			},
			expectedErr: "reference-transaction failure",
		},
		{
			desc: "post-receive error",
			preReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				return nil
			},
			update: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
				return nil
			},
			referenceTransaction: func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
				return nil
			},
			postReceive: func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
				_, err := io.Copy(stderr, strings.NewReader("post-receive failure"))
				require.NoError(t, err)
				return errors.New("ignored")
			},
			expectedErr:         "post-receive failure",
			expectedRefDeletion: true,
		},
	}

	for _, tc := range testCases {
		referenceTransactionCalls = 0
		t.Run(tc.desc, func(t *testing.T) {
			hookManager := hook.NewMockManager(t, tc.preReceive, tc.postReceive, tc.update, tc.referenceTransaction)

			gitCmdFactory := git.NewExecCommandFactory(cfg)
			updater := NewUpdaterWithHooks(cfg, hookManager, gitCmdFactory, nil)

			err := updater.UpdateReference(ctx, repo, user, nil, git.ReferenceName("refs/heads/master"), git.ZeroOID, git.ObjectID(oldRev))
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), tc.expectedErr)
			}

			if tc.expectedRefDeletion {
				contained, err := localrepo.NewTestRepo(t, cfg, repo).HasRevision(ctx, git.Revision("refs/heads/master"))
				require.NoError(t, err)
				require.False(t, contained, "branch should have been deleted")
				gittest.Exec(t, cfg, "-C", repoPath, "branch", "master", oldRev)
			} else {
				ref, err := localrepo.NewTestRepo(t, cfg, repo).GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, oldRev)
			}
		})
	}
}

func TestUpdaterWithHooks_quarantine(t *testing.T) {
	cfg, repoProto, _ := testcfg.BuildWithRepo(t)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	locator := config.NewLocator(cfg)

	ctx, cancel := testhelper.Context()
	defer cancel()

	unquarantinedRepo := localrepo.NewTestRepo(t, cfg, repoProto)

	quarantine, err := quarantine.New(ctx, repoProto, locator)
	require.NoError(t, err)
	quarantinedRepo := localrepo.NewTestRepo(t, cfg, quarantine.QuarantinedRepo())
	blobID, err := quarantinedRepo.WriteBlob(ctx, "", strings.NewReader("1834298812398123"))
	require.NoError(t, err)

	expectQuarantined := func(t *testing.T, env []string, quarantined bool) {
		t.Helper()

		if env != nil {
			payload, err := git.HooksPayloadFromEnv(env)
			require.NoError(t, err)
			if quarantined {
				testassert.ProtoEqual(t, quarantine.QuarantinedRepo(), payload.Repo)
			} else {
				testassert.ProtoEqual(t, repoProto, payload.Repo)
			}
		}

		exists, err := quarantinedRepo.HasRevision(ctx, blobID.Revision()+"^{blob}")
		require.NoError(t, err)
		require.Equal(t, quarantined, exists)

		exists, err = unquarantinedRepo.HasRevision(ctx, blobID.Revision()+"^{blob}")
		require.NoError(t, err)
		require.Equal(t, !quarantined, exists)
	}

	hookExecutions := make(map[string]int)
	hookManager := hook.NewMockManager(t,
		// The pre-receive hook is not expected to have the object in the normal repo.
		func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
			expectQuarantined(t, env, true)
			testassert.ProtoEqual(t, quarantine.QuarantinedRepo(), repo)
			hookExecutions["prereceive"]++
			return nil
		},
		// But the post-receive hook shall get the unquarantined repository as input, with
		// objects already having been migrated into the target repo.
		func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
			expectQuarantined(t, env, false)
			testassert.ProtoEqual(t, repoProto, repo)
			hookExecutions["postreceive"]++
			return nil
		},
		// The update hook gets executed after the pre-receive hook and will be executed for
		// each reference that we're updating. As it is called immediately before the ref
		// gets queued for update, objects must have already been migrated or otherwise
		// updating the refs will fail due to missing objects.
		func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
			expectQuarantined(t, env, false)
			testassert.ProtoEqual(t, quarantine.QuarantinedRepo(), repo)
			hookExecutions["update"]++
			return nil
		},
		// The reference-transaction hook is called as we're queueing refs for update, so
		// the objects must be part of the main object database or otherwise the update will
		// fail due to missing objects.
		func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
			expectQuarantined(t, env, false)
			switch state {
			case hook.ReferenceTransactionPrepared:
				hookExecutions["prepare"]++
			case hook.ReferenceTransactionCommitted:
				hookExecutions["commit"]++
			}
			return nil
		},
	)

	require.NoError(t, NewUpdaterWithHooks(cfg, hookManager, gitCmdFactory, nil).UpdateReference(
		ctx,
		repoProto,
		&gitalypb.User{
			GlId:       "1234",
			GlUsername: "Username",
			Name:       []byte("Name"),
			Email:      []byte("mail@example.com"),
		},
		quarantine,
		git.ReferenceName("refs/heads/master"),
		git.ZeroOID,
		git.ObjectID("1e292f8fedd741b75372e19097c76d327140c312"),
	))

	require.Equal(t, map[string]int{
		"prereceive":  1,
		"postreceive": 1,
		"update":      1,
		"prepare":     1,
		"commit":      1,
	}, hookExecutions)

	contained, err := unquarantinedRepo.HasRevision(ctx, git.Revision("refs/heads/master"))
	require.NoError(t, err)
	require.False(t, contained, "branch should have been deleted")
}
