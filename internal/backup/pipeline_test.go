package backup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestPipeline_Create(t *testing.T) {
	testPipelineCreate(t, func(strategy Strategy) CreatePipeline {
		return NewPipeline(logrus.StandardLogger(), strategy)
	})
}

func TestPipeline_Restore(t *testing.T) {
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
	p := NewPipeline(logrus.StandardLogger(), strategy)

	ctx, cancel := testhelper.Context()
	defer cancel()

	requests := []RestoreRequest{
		{Repository: &gitalypb.Repository{StorageName: "normal"}},
		{Repository: &gitalypb.Repository{StorageName: "skip"}},
		{Repository: &gitalypb.Repository{StorageName: "error"}},
	}
	for _, req := range requests {
		p.Restore(ctx, &req)
	}
	err := p.Done()
	require.EqualError(t, err, "pipeline: 1 failures encountered")
}

func TestParallelCreatePipeline(t *testing.T) {
	testPipelineCreate(t, func(strategy Strategy) CreatePipeline {
		return NewParallelCreatePipeline(NewPipeline(logrus.StandardLogger(), strategy), 2)
	})

	t.Run("parallelism", func(t *testing.T) {
		var calls int64
		strategy := MockStrategy{
			CreateFunc: func(ctx context.Context, req *CreateRequest) error {
				currentCalls := atomic.AddInt64(&calls, 1)
				assert.LessOrEqual(t, currentCalls, int64(2))

				time.Sleep(time.Millisecond)
				atomic.AddInt64(&calls, -1)
				return nil
			},
		}
		var p CreatePipeline
		p = NewPipeline(logrus.StandardLogger(), strategy)
		p = NewParallelCreatePipeline(p, 2)

		ctx, cancel := testhelper.Context()
		defer cancel()

		for i := 0; i < 5; i++ {
			p.Create(ctx, &CreateRequest{Repository: &gitalypb.Repository{StorageName: "default"}})
		}
		require.NoError(t, p.Done())
	})

	t.Run("context done", func(t *testing.T) {
		var p CreatePipeline
		p = NewPipeline(logrus.StandardLogger(), MockStrategy{})
		p = NewParallelCreatePipeline(p, 0) // make sure worker channels always block

		ctx, cancel := testhelper.Context()

		cancel()
		<-ctx.Done()

		p.Create(ctx, &CreateRequest{Repository: &gitalypb.Repository{StorageName: "default"}})

		err := p.Done()
		require.EqualError(t, err, "pipeline: context canceled")
	})
}

func TestParallelStorageCreatePipeline(t *testing.T) {
	testPipelineCreate(t, func(strategy Strategy) CreatePipeline {
		return NewParallelStorageCreatePipeline(NewPipeline(logrus.StandardLogger(), strategy), 2)
	})

	t.Run("parallelism", func(t *testing.T) {
		var calls int64
		strategy := MockStrategy{
			CreateFunc: func(ctx context.Context, req *CreateRequest) error {
				currentCalls := atomic.AddInt64(&calls, 1)
				// 3 storages by max 2 parallel
				assert.LessOrEqual(t, currentCalls, int64(3*2))

				time.Sleep(time.Millisecond)
				atomic.AddInt64(&calls, -1)
				return nil
			},
		}
		var p CreatePipeline
		p = NewPipeline(logrus.StandardLogger(), strategy)
		p = NewParallelStorageCreatePipeline(p, 2)

		ctx, cancel := testhelper.Context()
		defer cancel()

		for i := 0; i < 3; i++ {
			p.Create(ctx, &CreateRequest{Repository: &gitalypb.Repository{StorageName: "storage1"}})
			p.Create(ctx, &CreateRequest{Repository: &gitalypb.Repository{StorageName: "storage2"}})
			p.Create(ctx, &CreateRequest{Repository: &gitalypb.Repository{StorageName: "storage3"}})
		}
		require.NoError(t, p.Done())
	})

	t.Run("context done", func(t *testing.T) {
		var p CreatePipeline
		p = NewPipeline(logrus.StandardLogger(), MockStrategy{})
		p = NewParallelStorageCreatePipeline(p, 0) // make sure worker channels always block

		ctx, cancel := testhelper.Context()

		cancel()
		<-ctx.Done()

		p.Create(ctx, &CreateRequest{Repository: &gitalypb.Repository{StorageName: "default"}})

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

func testPipelineCreate(t *testing.T, init func(Strategy) CreatePipeline) {
	t.Run("strategy errors", func(t *testing.T) {
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
		p := init(strategy)

		ctx, cancel := testhelper.Context()
		defer cancel()

		requests := []CreateRequest{
			{Repository: &gitalypb.Repository{StorageName: "normal"}},
			{Repository: &gitalypb.Repository{StorageName: "skip"}},
			{Repository: &gitalypb.Repository{StorageName: "error"}},
		}
		for i := range requests {
			p.Create(ctx, &requests[i])
		}
		err := p.Done()
		require.EqualError(t, err, "pipeline: 1 failures encountered")
	})
}
