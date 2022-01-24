package featureflag

// ConcurrencyQueueEnforceMax enforces a maximum number of items that are waiting in a concurrency queue.
// when this flag is turned on, subsequent requests that come in will be rejected with an error.
var ConcurrencyQueueEnforceMax = NewFeatureFlag("concurrency_queue_enforce_max", false)
