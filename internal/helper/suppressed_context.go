package helper

import (
	"context"
	"time"
)

// suppressedContext suppresses cancellation or expiration of the context.
type suppressedContext struct{ context.Context }

func (suppressedContext) Deadline() (deadline time.Time, ok bool) { return time.Time{}, false }

func (suppressedContext) Done() <-chan struct{} { return nil }

func (suppressedContext) Err() error { return nil }

// SuppressCancellation returns a context that suppresses cancellation or expiration of the parent context.
func SuppressCancellation(ctx context.Context) context.Context { return suppressedContext{ctx} }
