package micro

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

var (
	// Returned when an inconsistent number of results is returned by the batch processor.
	ErrBadResults = errors.New("bad result from batch processor")
)

// BatchProcessor is a batch processing interface used by the Batcher to process batches.
//
// Implementations of BatchProcessor should ensure that the Process method always
// returns results in the same order in which they are requested.
type BatchProcessor[R, Q any] interface {
	Process(context.Context, []R) ([]Q, error)
}

// Batcher is a service that processes incoming jobs in grouped
// 'micro batches' according to the set of configurable thresholds.
type Batcher[R, Q any] struct {
	processor        BatchProcessor[R, Q]
	requestQueue     chan jobRequest[R, Q]
	stop             chan struct{}
	config           BatcherConfig
	requestBuffer    *Buffer[jobRequest[R, Q]]
	concurrencyLimit *semaphore.Weighted
	wg               sync.WaitGroup
}

// NewBatcher creates a new batcher.
// The default BatcherConfig is used as fallback.
func NewBatcher[R any, Q any](processor BatchProcessor[R, Q], opts ...Option) Batcher[R, Q] {
	return NewBatcherWithConfig(processor, DefaultBatcherConfig, opts...)
}

// NewBatcher creates a new batcher with the given config.
func NewBatcherWithConfig[R any, Q any](processor BatchProcessor[R, Q], config BatcherConfig, opts ...Option) Batcher[R, Q] {
	for _, o := range opts {
		o(&config)
	}
	return Batcher[R, Q]{
		processor:        processor,
		requestQueue:     make(chan jobRequest[R, Q], config.BufferSize),
		config:           config,
		concurrencyLimit: semaphore.NewWeighted(int64(config.InflightBatchLimit)),
		stop:             make(chan struct{}),
		requestBuffer:    newBuffer[jobRequest[R, Q]](),
	}
}

// Submit submits the job to the request queue, returning a JobResult.
// The function is non-blocking as long as the internal request queue is not full.
//
// This method is safe for concurrent use.
func (m *Batcher[R, Q]) Submit(j Job[R]) JobResult[Q] {
	result := newJobResult[Q](j.ID())
	jobRequest := newJobRequest(j, result.result)
	m.requestQueue <- jobRequest

	return result
}

// Start begins the Batcher's service loop.
func (m *Batcher[R, Q]) Start(ctx context.Context) {
	go m.main(ctx)
}

// Stop closes the internal request queue and waits for the service loop to complete shutdown.
func (m *Batcher[R, Q]) Stop() {
	close(m.requestQueue)
	<-m.stop
}

// main is Batcher's service loop.
// Its primary function is to accept requests from the queue and place them into the internal buffer.
// The service loop also controls the ticker responsible for the batchers duration threshold.
//
// If the context is cancelled the service loop will exit immediately.
// If the request queue is closed it will dispatch remaining items and wait for any processes to
// complete before exiting.
func (m *Batcher[R, Q]) main(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(m.config.DurationThreshold)
	for {
		select {
		case <-ticker.C:
			m.dispatch(ctx)
		case request, ok := <-m.requestQueue:
			if !ok {
				m.dispatch(ctx) // force-dispatch remaining requests
				m.wg.Wait()     // wait for in-flight batches to finish processing
				close(m.stop)
				return
			}
			m.enqueue(ctx, request)
		case <-ctx.Done():
			return
		}
	}
}

// enqueue adds a job request to the internal buffer and signals a dispatch if the size threshold has been exceeded.
func (m *Batcher[R, Q]) enqueue(ctx context.Context, r jobRequest[R, Q]) {
	m.requestBuffer.Add(r)

	if m.requestBuffer.Size() >= m.config.SizeThreshold {
		m.dispatch(ctx)
	}
}

// dispatch flushes the internal buffer and executes the batch processor on the flushed items.
// dispatch is non-blocking as long as the in-flight batch limit hasn't been hit.
//
// Each call to the batch processor is made in a separate go routine. This go routine is also
// responsible to sending the results to the corresponding result channels
func (m *Batcher[R, Q]) dispatch(ctx context.Context) {
	if m.requestBuffer.Size() == 0 {
		return
	}

	err := m.concurrencyLimit.Acquire(ctx, 1)
	if err != nil {
		// safe (enough) to ignore since the buffer has not been flushed yet
		slog.Error("failed to acquire on concurrency limit", slog.Any("err", err))
		return
	}

	jobRequests := m.requestBuffer.Flush()

	m.wg.Add(1)
	go func() {
		defer func() {
			m.concurrencyLimit.Release(1)
			m.wg.Done()
		}()
		requests := func() []R {
			ret := make([]R, len(jobRequests))
			for i, job := range jobRequests {
				ret[i] = job.delegate.Request
			}
			return ret
		}()
		results, err := m.processor.Process(ctx, requests)
		switch {
		case err != nil:
			err = fmt.Errorf("failed to run batch processor: %w", err)
		case len(results) != len(requests):
			err = ErrBadResults
		default:
			// okay
		}
		if err != nil {
			sendError(jobRequests, err)
			return
		}
		sendResults(jobRequests, results)
	}()
}

// sendError sends an error result message to each request's result channel.
func sendError[R, Q any](requests []jobRequest[R, Q], err error) {
	for _, request := range requests {
		request.SendResult(jobResultMessage[Q]{Err: err})
	}
}

// sendResults sends a result message to each request's result channel.
//
// The input requests and results arrays must be ordered the same.
func sendResults[R, Q any](requests []jobRequest[R, Q], results []Q) {
	for i, request := range requests {
		request.SendResult(jobResultMessage[Q]{Result: results[i]})
	}
}
