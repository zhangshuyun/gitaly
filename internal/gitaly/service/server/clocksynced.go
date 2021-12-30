package server

import (
	"context"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) ClockSynced(_ context.Context, req *gitalypb.ClockSyncedRequest) (*gitalypb.ClockSyncedResponse, error) {
	synced, err := helper.CheckClockSync(req.NtpHost, time.Duration(req.DriftThresholdMillis*int64(time.Millisecond)))
	if err != nil {
		return nil, err
	}
	return &gitalypb.ClockSyncedResponse{Synced: synced}, nil
}
