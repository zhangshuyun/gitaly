package backup

import (
	"context"
	"testing"

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
