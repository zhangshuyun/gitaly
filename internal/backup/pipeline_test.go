package backup

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestLoggingPipeline(t *testing.T) {
	testPipeline(t, func() Pipeline {
		return NewLoggingPipeline(logrus.StandardLogger())
	})
}

func TestParallelPipeline(t *testing.T) {
	testPipeline(t, func() Pipeline {
		return NewParallelPipeline(NewLoggingPipeline(logrus.StandardLogger()), 2, 0)
	})

	t.Run("parallelism", func(t *testing.T) {
		for _, tc := range []struct {
			parallel            int
			parallelStorage     int
			expectedMaxParallel int64
		}{
			{
				parallel:            2,
				parallelStorage:     0,
				expectedMaxParallel: 2,
			},
			{
				parallel:            2,
				parallelStorage:     3,
				expectedMaxParallel: 2,
			},
			{
				parallel:            0,
				parallelStorage:     3,
				expectedMaxParallel: 6, // 2 storages * 3 workers per storage
			},
		} {
			t.Run(fmt.Sprintf("parallel:%d,parallelStorage:%d", tc.parallel, tc.parallelStorage), func(t *testing.T) {
				var calls int64
				strategy := MockStrategy{
					CreateFunc: func(ctx context.Context, req *CreateRequest) error {
						currentCalls := atomic.AddInt64(&calls, 1)
						defer atomic.AddInt64(&calls, -1)

						assert.LessOrEqual(t, currentCalls, tc.expectedMaxParallel)

						time.Sleep(time.Millisecond)
						return nil
					},
				}
				var p Pipeline
				p = NewLoggingPipeline(logrus.StandardLogger())
				p = NewParallelPipeline(p, tc.parallel, tc.parallelStorage)

				ctx, cancel := testhelper.Context()
				defer cancel()

				for i := 0; i < 10; i++ {
					p.Handle(ctx, NewCreateCommand(strategy, storage.ServerInfo{}, &gitalypb.Repository{StorageName: "storage1"}, false))
					p.Handle(ctx, NewCreateCommand(strategy, storage.ServerInfo{}, &gitalypb.Repository{StorageName: "storage2"}, false))
				}
				require.NoError(t, p.Done())
			})
		}
	})

	t.Run("context done", func(t *testing.T) {
		var strategy MockStrategy
		var p Pipeline
		p = NewLoggingPipeline(logrus.StandardLogger())
		p = NewParallelPipeline(p, 0, 0) // make sure worker channels always block

		ctx, cancel := testhelper.Context()

		cancel()
		<-ctx.Done()

		p.Handle(ctx, NewCreateCommand(strategy, storage.ServerInfo{}, &gitalypb.Repository{StorageName: "default"}, false))

		err := p.Done()
		require.EqualError(t, err, "pipeline: context canceled")
	})
}

type MockStrategy struct {
	CreateFunc  func(context.Context, *CreateRequest) error
	RestoreFunc func(context.Context, *RestoreRequest) error
}

func (s MockStrategy) Create(ctx context.Context, req *CreateRequest) error {
	if s.CreateFunc != nil {
		return s.CreateFunc(ctx, req)
	}
	return nil
}

func (s MockStrategy) Restore(ctx context.Context, req *RestoreRequest) error {
	if s.RestoreFunc != nil {
		return s.RestoreFunc(ctx, req)
	}
	return nil
}

func testPipeline(t *testing.T, init func() Pipeline) {
	t.Run("create command", func(t *testing.T) {
		strategy := MockStrategy{
			CreateFunc: func(_ context.Context, req *CreateRequest) error {
				switch req.Repository.StorageName {
				case "normal":
					return nil
				case "skip":
					return ErrSkipped
				case "error":
					return assert.AnError
				}
				require.Failf(t, "unexpected call to Create", "StorageName = %q", req.Repository.StorageName)
				return nil
			},
		}
		p := init()

		ctx, cancel := testhelper.Context()
		defer cancel()

		commands := []Command{
			NewCreateCommand(strategy, storage.ServerInfo{}, &gitalypb.Repository{StorageName: "normal"}, false),
			NewCreateCommand(strategy, storage.ServerInfo{}, &gitalypb.Repository{StorageName: "skip"}, false),
			NewCreateCommand(strategy, storage.ServerInfo{}, &gitalypb.Repository{StorageName: "error"}, false),
		}
		for _, cmd := range commands {
			p.Handle(ctx, cmd)
		}
		err := p.Done()
		require.EqualError(t, err, "pipeline: 1 failures encountered")
	})

	t.Run("restore command", func(t *testing.T) {
		strategy := MockStrategy{
			RestoreFunc: func(_ context.Context, req *RestoreRequest) error {
				switch req.Repository.StorageName {
				case "normal":
					return nil
				case "skip":
					return ErrSkipped
				case "error":
					return assert.AnError
				}
				require.Failf(t, "unexpected call to Restore", "StorageName = %q", req.Repository.StorageName)
				return nil
			},
		}
		p := init()

		ctx, cancel := testhelper.Context()
		defer cancel()

		commands := []Command{
			NewRestoreCommand(strategy, storage.ServerInfo{}, &gitalypb.Repository{StorageName: "normal"}, false),
			NewRestoreCommand(strategy, storage.ServerInfo{}, &gitalypb.Repository{StorageName: "skip"}, false),
			NewRestoreCommand(strategy, storage.ServerInfo{}, &gitalypb.Repository{StorageName: "error"}, false),
		}
		for _, cmd := range commands {
			p.Handle(ctx, cmd)
		}
		err := p.Done()
		require.EqualError(t, err, "pipeline: 1 failures encountered")
	})
}
