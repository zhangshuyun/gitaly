package updateref

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

type mockHookManager struct {
	t                    *testing.T
	preReceive           func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error
	postReceive          func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error
	update               func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error
	referenceTransaction func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error
}

func (m *mockHookManager) PreReceiveHook(ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
	return m.preReceive(m.t, ctx, repo, pushOptions, env, stdin, stdout, stderr)
}

func (m *mockHookManager) PostReceiveHook(ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
	return m.postReceive(m.t, ctx, repo, pushOptions, env, stdin, stdout, stderr)
}

func (m *mockHookManager) UpdateHook(ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
	return m.update(m.t, ctx, repo, ref, oldValue, newValue, env, stdout, stderr)
}

func (m *mockHookManager) ReferenceTransactionHook(ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
	return m.referenceTransaction(m.t, ctx, state, env, stdin)
}

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

	updater := NewUpdaterWithHooks(cfg, &mockHookManager{}, nil, nil)

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
			err := updater.UpdateReference(ctx, repo, user, tc.ref, tc.newRev, tc.oldRev)
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
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory()))
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
				changes, err := ioutil.ReadAll(stdin)
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
				changes, err := ioutil.ReadAll(stdin)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%s %s refs/heads/master\n", oldRev, git.ZeroOID.String()), string(changes))
				require.Equal(t, env, expectedEnv)
				require.Empty(t, pushOptions)
				return nil
			},
			referenceTransaction: func(t *testing.T, ctx context.Context, state hook.ReferenceTransactionState, env []string, stdin io.Reader) error {
				changes, err := ioutil.ReadAll(stdin)
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
			hookManager := &mockHookManager{
				t:                    t,
				preReceive:           tc.preReceive,
				postReceive:          tc.postReceive,
				update:               tc.update,
				referenceTransaction: tc.referenceTransaction,
			}

			gitCmdFactory := git.NewExecCommandFactory(cfg)
			updater := NewUpdaterWithHooks(cfg, hookManager, gitCmdFactory, nil)

			err := updater.UpdateReference(ctx, repo, user, git.ReferenceName("refs/heads/master"), git.ZeroOID, git.ObjectID(oldRev))
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
