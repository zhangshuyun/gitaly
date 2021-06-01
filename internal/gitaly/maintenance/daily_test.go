package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestStartDaily(t *testing.T) {
	dw := NewDailyWorker()

	clockQ := make(chan time.Time)
	dw.clock = func() time.Time {
		return <-clockQ
	}

	timerQ := make(chan time.Time)
	durationQ := make(chan time.Duration)
	dw.timer = func(d time.Duration) <-chan time.Time {
		durationQ <- d
		return timerQ
	}

	storagesQ := make(chan []string)
	fn := func(_ context.Context, _ logrus.FieldLogger, s []string) error {
		storagesQ <- s
		return nil
	}

	errQ := make(chan error)
	s := config.DailyJob{
		Hour:     1,
		Duration: config.Duration(time.Hour),
		Storages: []string{"meow"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { errQ <- dw.StartDaily(ctx, testhelper.DiscardTestEntry(t), s, fn) }()

	startTime := time.Date(1999, 3, 31, 0, 0, 0, 0, time.Local)
	for _, tt := range []struct {
		name           string
		now            time.Time
		expectDuration time.Duration
	}{
		{
			name:           "next job in an hour",
			now:            startTime,
			expectDuration: time.Hour,
		},
		{
			name:           "next job tomorrow",
			now:            startTime.Add(time.Hour),
			expectDuration: 24 * time.Hour,
		},
		{
			name:           "next job tomorrow",
			now:            startTime.Add(25 * time.Hour),
			expectDuration: 24 * time.Hour,
		},
		{
			name:           "next job in less than 24 hours",
			now:            startTime.Add(25 * time.Hour).Add(time.Minute),
			expectDuration: 24*time.Hour - time.Minute,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clockQ <- tt.now                                 // start time
			clockQ <- tt.now                                 // time used to compute timer
			require.Equal(t, tt.expectDuration, <-durationQ) // wait time until job
			timerQ <- tt.now.Add(tt.expectDuration)          // trigger the job
			require.Equal(t, s.Storages, <-storagesQ)        // fn was invoked
		})
	}

	// abort daily task
	cancel()
	clockQ <- startTime // mock artifact; this value doesn't matter
	clockQ <- startTime // mock artifact; this value doesn't matter
	<-durationQ         // mock artifact; this value doesn't matter
	require.Equal(t, context.Canceled, <-errQ)
}
