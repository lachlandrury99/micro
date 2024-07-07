package micro_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lachlandrury99/micro"
)

type Request string
type Result string

type BatchProcessor struct{}

func (b *BatchProcessor) Process(ctx context.Context, requests []Request) ([]Result, error) {
	fmt.Printf("Processing batch: %v\n", requests)
	results := make([]Result, len(requests))
	for i, v := range requests {
		results[i] = Result(fmt.Sprintf("Processed: %s", v))
	}
	return results, nil
}

func Example() {
	var wg sync.WaitGroup

	ctx := context.Background()
	batcher := micro.NewBatcher(&BatchProcessor{}, micro.WithSizeThreshold(2))

	batcher.Start(ctx)

	requests := []Request{"a", "b", "c", "d", "e"}
	for _, request := range requests {
		wg.Add(1)

		job := micro.NewJob(request)
		jobResult := batcher.Submit(job)

		time.Sleep(time.Millisecond)

		go func() {
			defer wg.Done()
			result, err := jobResult.Get()
			if err != nil {
				panic(err)
			}
			fmt.Println(result)
		}()
	}

	batcher.Stop()

	wg.Wait()

	// Output:
	// Processing batch: [a b]
	// Processed: a
	// Processed: b
	// Processing batch: [c d]
	// Processed: c
	// Processed: d
	// Processing batch: [e]
	// Processed: e
}
