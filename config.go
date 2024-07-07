package micro

import "time"

// BatcherConfig is used to configure a Batcher.
type BatcherConfig struct {
	// DurationThreshold is the frequency of the batch processor.
	DurationThreshold time.Duration
	// SizeThreshold is the maximum number of buffered jobs before the batch processor
	// is executed.
	SizeThreshold int
	// InflightBatchLimit is the maximum number of batch processes that can execute
	// concurrently. This may be useful in cases where the batch processor is
	// IO bound.
	InflightBatchLimit int
	// BufferSize is the size of the job request queue. If the job request queue is full
	// then batcher.Submit will block until the queue has capacity again.
	BufferSize int
}

// DefaultBatcherConfig holds the default configuration for BatcherConfig.
var DefaultBatcherConfig = BatcherConfig{
	DurationThreshold:  time.Second,
	SizeThreshold:      10,
	InflightBatchLimit: 1,
	BufferSize:         100,
}

// Option is a function that applies a configuration to BatcherConfig
type Option func(*BatcherConfig)

// WithDurationThreshold configures BatcherConfig.DurationThreshold
func WithDurationThreshold(d time.Duration) Option {
	return func(bc *BatcherConfig) {
		bc.DurationThreshold = d
	}
}

// WithSizeThreshold configures BatcherConfig.SizeThreshold
func WithSizeThreshold(d int) Option {
	return func(bc *BatcherConfig) {
		bc.SizeThreshold = d
	}
}

// WithInflightBatchLimit configures BatcherConfig.InflightBatchLimit
func WithInflightBatchLimit(l int) Option {
	return func(bc *BatcherConfig) {
		bc.InflightBatchLimit = l
	}
}

// WithBufferSize configures BatcherConfig.BufferSize
func WithBufferSize(s int) Option {
	return func(bc *BatcherConfig) {
		bc.BufferSize = s
	}
}
