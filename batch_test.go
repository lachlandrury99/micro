package micro_test

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lachlandrury99/micro"
)

type MockRequest struct {
	ID              string
	ProcessingDelay time.Duration
}

type MockResult struct {
	ID          string
	BatchNumber uint64
}

type MockBatchLog struct {
	RequestIDs     []string
	StartIncrement uint64
	EndIncrement   uint64
}

type MockBatchProcessor struct {
	BatchCount          atomic.Uint64
	SequentialIncrement atomic.Uint64
	Logs                []MockBatchLog
	mu                  sync.Mutex
}

func (p *MockBatchProcessor) Process(ctx context.Context, r []MockRequest) ([]MockResult, error) {
	batchNumber := p.BatchCount.Add(1)
	startIncrement := p.SequentialIncrement.Add(1)

	ret := make([]MockResult, len(r))
	for i, v := range r {
		time.Sleep(v.ProcessingDelay)
		ret[i] = MockResult{
			ID:          v.ID,
			BatchNumber: batchNumber,
		}
	}

	endIncrement := p.SequentialIncrement.Add(1)

	log := MockBatchLog{
		RequestIDs: func() []string {
			ret := make([]string, len(r))
			for i, v := range r {
				ret[i] = v.ID
			}
			return ret
		}(),
		StartIncrement: startIncrement,
		EndIncrement:   endIncrement,
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.Logs = append(p.Logs, log)

	return ret, nil
}

type TestRequest struct {
	submitDelay    time.Duration
	request        MockRequest
	expectedResult MockResult
	result         micro.JobResult[MockResult]
}

type Test struct {
	name        string
	options     []micro.Option
	requests    []*TestRequest
	expectedLog []MockBatchLog
}

func (ts *Test) Run(t *testing.T) {
	ctx := context.Background()
	processor := &MockBatchProcessor{}
	batcher := micro.NewBatcher(
		processor,
		ts.options...,
	)

	batcher.Start(ctx)

	for _, v := range ts.requests {
		time.Sleep(v.submitDelay)
		v.result = batcher.Submit(micro.NewJob(v.request))
	}

	batcher.Stop()

	ts.assertBatchLogs(t, processor.Logs)
	ts.assertRequestResults(t)
}

func (ts *Test) assertRequestResults(t *testing.T) {
	for _, v := range ts.requests {
		result, err := v.result.Get()
		if err != nil {
			t.Errorf("unexpected error for result: %v", err)
		}
		if !resultEqual(result, v.expectedResult) {
			t.Errorf("unexpected result: got=%v, want=%v", result, v.expectedResult)
		}
	}
}

func (ts *Test) assertBatchLogs(t *testing.T, logs []MockBatchLog) {
	if len(ts.expectedLog) != len(logs) {
		t.Errorf("unexpected batch logs: got=%+v, want=%+v", logs, ts.expectedLog)
	}
	for i, actual := range logs {
		expected := ts.expectedLog[i]
		if !batchLogEqual(expected, actual) {
			t.Errorf("unexpected batch log: got=%+v, want=%+v", actual, expected)
		}
	}
}

func TestBatcher(t *testing.T) {
	tests := []Test{
		{
			name: "jobs are batched in groups of max two",
			options: []micro.Option{
				micro.WithDurationThreshold(time.Hour),
				micro.WithSizeThreshold(2),
			},
			requests: []*TestRequest{
				{
					request:        MockRequest{ID: "a"},
					expectedResult: MockResult{ID: "a", BatchNumber: 1},
				},
				{
					request:        MockRequest{ID: "b"},
					expectedResult: MockResult{ID: "b", BatchNumber: 1},
				},
				{
					request:        MockRequest{ID: "c"},
					expectedResult: MockResult{ID: "c", BatchNumber: 2},
				},
				{
					request:        MockRequest{ID: "d"},
					expectedResult: MockResult{ID: "d", BatchNumber: 2},
				},
				{
					request:        MockRequest{ID: "e"},
					expectedResult: MockResult{ID: "e", BatchNumber: 3},
				},
			},
			expectedLog: []MockBatchLog{
				{
					RequestIDs:     []string{"a", "b"},
					StartIncrement: 1,
					EndIncrement:   2,
				},
				{
					RequestIDs:     []string{"c", "d"},
					StartIncrement: 3,
					EndIncrement:   4,
				},
				{
					RequestIDs:     []string{"e"},
					StartIncrement: 5,
					EndIncrement:   6,
				},
			},
		},
		{
			name: "jobs are batched every second",
			options: []micro.Option{
				micro.WithDurationThreshold(time.Second),
				micro.WithSizeThreshold(10),
			},
			requests: []*TestRequest{
				{
					submitDelay:    time.Second,
					request:        MockRequest{ID: "a"},
					expectedResult: MockResult{ID: "a", BatchNumber: 1},
				},
				{
					submitDelay:    time.Second,
					request:        MockRequest{ID: "b"},
					expectedResult: MockResult{ID: "b", BatchNumber: 2},
				},
				{
					request:        MockRequest{ID: "c"},
					expectedResult: MockResult{ID: "c", BatchNumber: 2},
				},
				{
					submitDelay:    time.Second,
					request:        MockRequest{ID: "d"},
					expectedResult: MockResult{ID: "d", BatchNumber: 3},
				},
				{
					request:        MockRequest{ID: "e"},
					expectedResult: MockResult{ID: "e", BatchNumber: 3},
				},
			},
			expectedLog: []MockBatchLog{
				{
					RequestIDs:     []string{"a"},
					StartIncrement: 1,
					EndIncrement:   2,
				},
				{
					RequestIDs:     []string{"b", "c"},
					StartIncrement: 3,
					EndIncrement:   4,
				},
				{
					RequestIDs:     []string{"d", "e"},
					StartIncrement: 5,
					EndIncrement:   6,
				},
			},
		},
		{
			name: "jobs are processed concurrently (max 2) in batches of 2",
			options: []micro.Option{
				micro.WithDurationThreshold(time.Hour),
				micro.WithSizeThreshold(2),
				micro.WithInflightBatchLimit(2),
			},
			requests: []*TestRequest{
				{
					request:        MockRequest{ID: "a", ProcessingDelay: time.Millisecond * 100},
					expectedResult: MockResult{ID: "a", BatchNumber: 1},
				},
				{
					request:        MockRequest{ID: "b"},
					expectedResult: MockResult{ID: "b", BatchNumber: 1},
				},
				{
					submitDelay:    time.Millisecond * 50,
					request:        MockRequest{ID: "c", ProcessingDelay: time.Millisecond * 100},
					expectedResult: MockResult{ID: "c", BatchNumber: 2},
				},
				{
					request:        MockRequest{ID: "d", ProcessingDelay: time.Millisecond * 100},
					expectedResult: MockResult{ID: "d", BatchNumber: 2},
				},
				{
					submitDelay:    time.Millisecond * 50,
					request:        MockRequest{ID: "e", ProcessingDelay: time.Millisecond * 300},
					expectedResult: MockResult{ID: "e", BatchNumber: 3},
				},
				{
					request:        MockRequest{ID: "f"},
					expectedResult: MockResult{ID: "f", BatchNumber: 3},
				},
			},
			expectedLog: []MockBatchLog{
				{
					RequestIDs:     []string{"a", "b"},
					StartIncrement: 1,
					EndIncrement:   3,
				},
				{
					RequestIDs:     []string{"c", "d"},
					StartIncrement: 2,
					EndIncrement:   5,
				},
				{
					RequestIDs:     []string{"e", "f"},
					StartIncrement: 4,
					EndIncrement:   6,
				},
			},
		},
	}
	for _, ts := range tests {
		t.Run(ts.name, func(t *testing.T) {
			ts.Run(t)
		})
	}
}

func resultEqual(got, want MockResult) bool {
	if got.ID != want.ID {
		return false
	}
	if got.BatchNumber != want.BatchNumber {
		return false
	}
	return true
}

func batchLogEqual(got, want MockBatchLog) bool {
	if slices.Compare(got.RequestIDs, want.RequestIDs) != 0 {
		return false
	}
	if got.StartIncrement != want.StartIncrement {
		return false
	}
	if got.EndIncrement != want.EndIncrement {
		return false
	}
	return true
}
