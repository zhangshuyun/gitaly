// +build postgres

package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSetReplicationFactorSubcommand(t *testing.T) {
	for _, tc := range []struct {
		desc   string
		args   []string
		store  praefect.AssignmentStore
		error  error
		stdout string
	}{
		{
			desc:  "unexpected positional arguments",
			args:  []string{"positonal-arg"},
			error: unexpectedPositionalArgsError{Command: "set-replication-factor"},
		},
		{
			desc:  "missing virtual-storage",
			args:  []string{},
			error: requiredParameterError("virtual-storage"),
		},
		{
			desc:  "missing repository",
			args:  []string{"-virtual-storage=virtual-storage"},
			error: requiredParameterError("repository"),
		},
		{
			desc:  "missing replication-factor",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=relative-path"},
			error: requiredParameterError("replication-factor"),
		},
		{
			desc:  "replication factor too small",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=relative-path", "-replication-factor=0"},
			error: status.Error(codes.InvalidArgument, "set replication factor: attempted to set replication factor 0 but minimum is 1"),
		},
		{
			desc:  "replication factor too big",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=relative-path", "-replication-factor=3"},
			error: status.Error(codes.InvalidArgument, "set replication factor: attempted to set replication factor 3 but virtual storage only contains 2 storages"),
		},
		{
			desc:  "virtual storage not found",
			args:  []string{"-virtual-storage=non-existent", "-repository=relative-path", "-replication-factor=2"},
			error: status.Error(codes.InvalidArgument, `set replication factor: virtual storage "non-existent" not found`),
		},
		{
			desc:  "repository not found",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=non-existent", "-replication-factor=2"},
			error: status.Error(codes.InvalidArgument, `set replication factor: repository "virtual-storage"/"non-existent" not found`),
		},
		{
			desc:  "assignments are disabled",
			args:  []string{"-virtual-storage=virtual-storage", "-repository=relative-path", "-replication-factor=1"},
			store: praefect.NewDisabledAssignmentStore(nil),
			error: status.Error(codes.Internal, `set replication factor: assignments are disabled`),
		},
		{
			desc:   "successfully set",
			args:   []string{"-virtual-storage=virtual-storage", "-repository=relative-path", "-replication-factor=2"},
			stdout: "current assignments: primary, secondary\n",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db := getDB(t)

			store := tc.store
			if tc.store == nil {
				store = datastore.NewAssignmentStore(db, map[string][]string{"virtual-storage": {"primary", "secondary"}})
			}

			// create a repository record
			require.NoError(t,
				datastore.NewPostgresRepositoryStore(db, nil).SetGeneration(ctx, "virtual-storage", "relative-path", "primary", 0),
			)

			ln, clean := listenAndServe(t, []svcRegistrar{registerPraefectInfoServer(
				info.NewServer(nil, config.Config{}, nil, nil, store, nil, nil),
			)})
			defer clean()

			stdout := &bytes.Buffer{}
			cmd := &setReplicationFactorSubcommand{stdout: stdout}
			fs := cmd.FlagSet()
			require.NoError(t, fs.Parse(tc.args))
			err := cmd.Exec(fs, config.Config{
				SocketPath: ln.Addr().String(),
			})
			require.Equal(t, tc.error, err)
			require.Equal(t, tc.stdout, stdout.String())
		})
	}
}
