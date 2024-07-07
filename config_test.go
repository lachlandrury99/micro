package micro

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

type NoopProcessor struct{}

func (np *NoopProcessor) Process(ctx context.Context, requests []interface{}) ([]interface{}, error) {
	return nil, nil
}

func TestBatcherOptions(t *testing.T) {
	var (
		testDurationThreshold = time.Second * time.Duration(rand.Intn(60))
		testSizeThreshold     = rand.Intn(1000)
		testBufferSize        = rand.Intn(1000)
		testInflightLimit     = rand.Intn(1000)
	)
	batcher := NewBatcher(
		&NoopProcessor{},
		WithDurationThreshold(testDurationThreshold),
		WithSizeThreshold(testSizeThreshold),
		WithInflightBatchLimit(testInflightLimit),
		WithBufferSize(testBufferSize),
	)

	if batcher.config.DurationThreshold != testDurationThreshold {
		t.Errorf("unexpected duration threshold: want=%s, got=%s", testDurationThreshold, batcher.config.DurationThreshold)
	}

	if batcher.config.SizeThreshold != testSizeThreshold {
		t.Errorf("unexpected size threshold: want=%d, got=%d", testSizeThreshold, batcher.config.SizeThreshold)
	}

	if batcher.config.InflightBatchLimit != testInflightLimit {
		t.Errorf("unexpected inflight batch limit: want=%d, got=%d", testInflightLimit, batcher.config.InflightBatchLimit)
	}

	if batcher.config.BufferSize != testBufferSize {
		t.Errorf("unexpected buffer size: want=%d, got=%d", testBufferSize, batcher.config.BufferSize)
	}
}
