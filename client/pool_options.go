package client

import "google.golang.org/grpc"

type poolOptions struct {
	dialer      Dialer
	dialOptions []grpc.DialOption
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
type PoolOption func(*poolOptions)

func applyPoolOptions(options []PoolOption) *poolOptions {
	opts := defaultPoolOptions()
	for _, opt := range options {
		opt(opts)
	}
	return opts
}

func defaultPoolOptions() *poolOptions {
	return &poolOptions{
		dialer: DialContext,
	}
}

// WithDialer sets the dialer that is called for each new gRPC connection the pool establishes.
func WithDialer(dialer Dialer) PoolOption {
	return func(options *poolOptions) {
		options.dialer = dialer
	}
}

// WithDialOptions sets gRPC options to use for the gRPC Dial call.
func WithDialOptions(dialOptions ...grpc.DialOption) PoolOption {
	return func(options *poolOptions) {
		options.dialOptions = dialOptions
	}
}
