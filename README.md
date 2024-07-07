# Micro
Micro is a go micro-batching library.

# What is micro-batching? 
Miro-batching is a batching technique aimed at balancing processing throughput and system overhead. It achieves this by grouping tasks into small batches that can be processed collectively by downstream systems.

# Usage
See [here](example_test.go) for an example on how to use the library.

# Design
The library is designed as a utility wrapper for function-specific batch processors. The library is able to utilize batch processors that implement the exposed interface `BatchProcessor`. The batching service provided handles the ingestion/buffering of incoming jobs and executes the batch processor as required; based on the configuration set.







