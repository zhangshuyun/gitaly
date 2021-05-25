package maintenance

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/dontpanic"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// StoragesJob runs a job on storages. The string slice param indicates which
// storages are currently enabled for the feature.
type StoragesJob func(context.Context, logrus.FieldLogger, []string) error

// DailyWorker allows for a storage job to be executed on a daily schedule
type DailyWorker struct {
	// clock allows the time telling to be overridden deterministically in unit tests
	clock func() time.Time
	// timer allows the timing of tasks to be overridden deterministically in unit tests
	timer func(time.Duration) <-chan time.Time
}

// NewDailyWorker returns an initialized daily worker
func NewDailyWorker() DailyWorker {
	return DailyWorker{
		clock: time.Now,
		timer: time.After,
	}
}

func (dw DailyWorker) nextTime(hour, minute int) time.Time {
	n := dw.clock()
	next := time.Date(n.Year(), n.Month(), n.Day(), hour, minute, 0, 0, n.Location())
	if next.Equal(n) || next.Before(n) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

// StartDaily will run the provided job every day at the specified time for the
// specified duration. Only the specified storages wil be worked on.
func (dw DailyWorker) StartDaily(ctx context.Context, l logrus.FieldLogger, schedule config.DailyJob, job StoragesJob) error {
	if schedule.Duration == 0 || len(schedule.Storages) == 0 || schedule.Disabled {
		return nil
	}

	for {
		nt := dw.nextTime(int(schedule.Hour), int(schedule.Minute))
		l.WithField("scheduled", nt).Info("maintenance: daily scheduled")

		var start time.Time

		select {
		case <-ctx.Done():
			return ctx.Err()
		case start = <-dw.timer(nt.Sub(dw.clock())):
			l.WithField("max_duration", schedule.Duration.Duration()).
				Info("maintenance: daily starting")
		}

		var jobErr error
		dontpanic.Try(func() {
			ctx, cancel := context.WithTimeout(ctx, schedule.Duration.Duration())
			defer cancel()

			jobErr = job(ctx, l, schedule.Storages)
		})

		l.WithError(jobErr).
			WithField("max_duration", schedule.Duration.Duration()).
			WithField("actual_duration", time.Since(start)).
			Info("maintenance: daily completed")
	}
}
