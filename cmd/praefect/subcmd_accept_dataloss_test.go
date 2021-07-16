// +build postgres

package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestAcceptDatalossSubcommand(t *testing.T) {
	const (
		vs   = "test-virtual-storage-1"
		repo = "test-repository-1"
		st1  = "test-physical-storage-1"
		st2  = "test-physical-storage-2"
		st3  = "test-physical-storage-3"
	)

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name:  vs,
				Nodes: []*config.Node{{Storage: st1}, {Storage: st2}, {Storage: st3}},
			},
		},
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	rs := datastore.NewPostgresRepositoryStore(getDB(t), conf.StorageNames())
	startingGenerations := map[string]int{st1: 1, st2: 0, st3: datastore.GenerationUnknown}
	for storage, generation := range startingGenerations {
		if generation == datastore.GenerationUnknown {
			continue
		}

		require.NoError(t, rs.SetGeneration(ctx, vs, repo, storage, generation))
	}

	q := &datastore.MockReplicationEventQueue{
		EnqueueFunc: func(ctx context.Context, event datastore.ReplicationEvent) (datastore.ReplicationEvent, error) {
			if event.Job.TargetNodeStorage == st2 {
				return event, fmt.Errorf("replication event scheduled for authoritative storage %q", st2)
			}

			generation, err := rs.GetGeneration(ctx, event.Job.VirtualStorage, event.Job.RelativePath, event.Job.SourceNodeStorage)
			if err != nil {
				return event, err
			}

			return event, rs.SetGeneration(ctx, event.Job.VirtualStorage, event.Job.RelativePath, event.Job.TargetNodeStorage, generation)
		},
	}

	ln, clean := listenAndServe(t, []svcRegistrar{registerPraefectInfoServer(info.NewServer(conf, q, rs, nil, nil, nil))})
	defer clean()

	conf.SocketPath = ln.Addr().String()

	type errorMatcher func(t *testing.T, err error)

	matchEqual := func(expected error) errorMatcher {
		return func(t *testing.T, actual error) {
			t.Helper()
			require.Equal(t, expected, actual)
		}
	}

	matchNoError := func() errorMatcher {
		return func(t *testing.T, actual error) {
			t.Helper()
			require.NoError(t, actual)
		}
	}

	matchErrorMsg := func(expected string) errorMatcher {
		return func(t *testing.T, actual error) {
			t.Helper()
			require.EqualError(t, actual, expected)
		}
	}

	for _, tc := range []struct {
		desc                string
		args                []string
		virtualStorages     []*config.VirtualStorage
		datalossCheck       func(context.Context, *gitalypb.DatalossCheckRequest) (*gitalypb.DatalossCheckResponse, error)
		output              string
		matchError          errorMatcher
		expectedGenerations map[string]int
	}{
		{
			desc:                "missing virtual storage",
			args:                []string{},
			matchError:          matchEqual(requiredParameterError(paramVirtualStorage)),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "missing repository",
			args:                []string{"-virtual-storage=test-virtual-storage-1"},
			matchError:          matchEqual(requiredParameterError(paramRelativePath)),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "missing authoritative storage",
			args:                []string{"-virtual-storage=test-virtual-storage-1", "-repository=test-repository-1"},
			matchError:          matchEqual(requiredParameterError(paramAuthoritativeStorage)),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "positional arguments",
			args:                []string{"-virtual-storage=test-virtual-storage-1", "-repository=test-repository-1", "-authoritative-storage=test-physical-storage-2", "positional-arg"},
			matchError:          matchEqual(unexpectedPositionalArgsError{Command: "accept-dataloss"}),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "non-existent virtual storage",
			args:                []string{"-virtual-storage=non-existent", "-repository=test-repository-1", "-authoritative-storage=test-physical-storage-2"},
			matchError:          matchErrorMsg(`set authoritative storage: rpc error: code = InvalidArgument desc = unknown virtual storage: "non-existent"`),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "non-existent authoritative storage",
			args:                []string{"-virtual-storage=test-virtual-storage-1", "-repository=test-repository-1", "-authoritative-storage=non-existent"},
			matchError:          matchErrorMsg(`set authoritative storage: rpc error: code = InvalidArgument desc = unknown authoritative storage: "non-existent"`),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "non-existent repository",
			args:                []string{"-virtual-storage=test-virtual-storage-1", "-repository=non-existent", "-authoritative-storage=test-physical-storage-2"},
			matchError:          matchErrorMsg(`set authoritative storage: rpc error: code = InvalidArgument desc = repository "non-existent" does not exist on virtual storage "test-virtual-storage-1"`),
			expectedGenerations: startingGenerations,
		},
		{
			desc:                "success",
			args:                []string{"-virtual-storage=test-virtual-storage-1", "-repository=test-repository-1", "-authoritative-storage=test-physical-storage-2"},
			matchError:          matchNoError(),
			expectedGenerations: map[string]int{st1: 2, st2: 2, st3: 2},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			cmd := &acceptDatalossSubcommand{}
			fs := cmd.FlagSet()
			require.NoError(t, fs.Parse(tc.args))
			tc.matchError(t, cmd.Exec(fs, conf))
			for storage, expected := range tc.expectedGenerations {
				actual, err := rs.GetGeneration(ctx, vs, repo, storage)
				require.NoError(t, err)
				require.Equal(t, expected, actual, storage)
			}
		})
	}
}
