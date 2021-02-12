package operations

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	hookservice "gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

func TestUpdateReferenceWithHooks_invalidParameters(t *testing.T) {
	repo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	user := &gitalypb.User{
		GlId:       "1234",
		GlUsername: "Username",
		Name:       []byte("Name"),
		Email:      []byte("mail@example.com"),
	}

	revA, revB := strings.Repeat("a", 40), strings.Repeat("b", 40)

	server := NewServer(config.Config, nil, &mockHookManager{}, nil, nil, nil)

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc                string
		ref, newRev, oldRev string
		expectedErr         string
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
			err := server.updateReferenceWithHooks(ctx, repo, user, tc.ref, tc.newRev, tc.oldRev)
			require.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

func TestUpdateReferenceWithHooks(t *testing.T) {
	server := testhelper.NewServer(t, nil, nil, testhelper.WithInternalSocket(config.Config))
	defer server.Stop()

	// We need to set up a separate "real" hook service here, as it will be used in
	// git-update-ref(1) spawned by `updateRefWithHooks()`
	txManager := transaction.NewManager(config.Config)
	hookManager := hook.NewManager(config.NewLocator(config.Config), txManager, hook.GitlabAPIStub, config.Config)
	gitCmdFactory := git.NewExecCommandFactory(config.Config)
	gitalypb.RegisterHookServiceServer(server.GrpcServer(), hookservice.NewServer(config.Config, hookManager, gitCmdFactory))
	server.Start(t)

	user := &gitalypb.User{
		GlId:       "1234",
		GlUsername: "Username",
		Name:       []byte("Name"),
		Email:      []byte("mail@example.com"),
	}

	oldRev := "1e292f8fedd741b75372e19097c76d327140c312"

	ctx, cancel := testhelper.Context()
	defer cancel()

	repo, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	payload, err := git.NewHooksPayload(config.Config, repo, nil, nil, &git.ReceiveHooksPayload{
		UserID:   "1234",
		Username: "Username",
		Protocol: "web",
	}).Env()
	require.NoError(t, err)

	expectedEnv := []string{
		payload,
	}

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
				require.Equal(t, state, hook.ReferenceTransactionPrepared)
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
		t.Run(tc.desc, func(t *testing.T) {
			hookManager := &mockHookManager{
				t:                    t,
				preReceive:           tc.preReceive,
				postReceive:          tc.postReceive,
				update:               tc.update,
				referenceTransaction: tc.referenceTransaction,
			}

			hookServer := NewServer(config.Config, nil, hookManager, nil, nil, gitCmdFactory)

			err := hookServer.updateReferenceWithHooks(ctx, repo, user, "refs/heads/master", git.ZeroOID.String(), oldRev)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), tc.expectedErr)
			}

			if tc.expectedRefDeletion {
				contained, err := localrepo.New(gitCmdFactory, repo, config.Config).HasRevision(ctx, git.Revision("refs/heads/master"))
				require.NoError(t, err)
				require.False(t, contained, "branch should have been deleted")
				testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "branch", "master", oldRev)
			} else {
				ref, err := localrepo.New(gitCmdFactory, repo, config.Config).GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, oldRev)
			}
		})
	}
}
