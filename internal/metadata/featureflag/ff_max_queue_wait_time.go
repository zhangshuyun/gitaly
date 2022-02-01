package featureflag

// ConcurrencyQueueMaxWait will enable the concurrency limiter to drop requests that are waiting in
// the concurrency queue for longer than the configured time.
var ConcurrencyQueueMaxWait = NewFeatureFlag("concurrency_queue_max_wait", false)
