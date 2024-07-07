package micro

import "github.com/google/uuid"

// Job is a request wrapper
type Job[R any] struct {
	id      string
	Request R
}

// NewJob returns a new job
func NewJob[R any](request R) Job[R] {
	return Job[R]{id: uuid.NewString(), Request: request}
}

// ID returns a unique identifier for the job
func (j Job[R]) ID() string {
	return j.id
}

// JobResult is a result type that contains a channel for the expected result
type JobResult[Q any] struct {
	JobID  string
	result chan jobResultMessage[Q]
}

// newJobResult returns a new JobResult with an empty result channel
func newJobResult[Q any](id string) JobResult[Q] {
	return JobResult[Q]{
		JobID: id,
		// result channel is buffered to ensure processor doesn't block when sending result
		result: make(chan jobResultMessage[Q], 1),
	}
}

// Get waits for the result to be received from the batcher
func (j *JobResult[Q]) Get() (Q, error) {
	msg := <-j.result
	return msg.Result, msg.Err
}

// jobRequest is a wrapper of Job that additionally contains the job's result channel
type jobRequest[R, Q any] struct {
	delegate      Job[R]
	resultChannel chan jobResultMessage[Q]
}

// newJobRequest creates a new jobRequest
func newJobRequest[R, Q any](job Job[R], resultChannel chan jobResultMessage[Q]) jobRequest[R, Q] {
	return jobRequest[R, Q]{
		delegate:      job,
		resultChannel: resultChannel,
	}
}

// SendResult sends the message to the job result channel
func (jr jobRequest[R, Q]) SendResult(message jobResultMessage[Q]) {
	jr.resultChannel <- message
}

type jobResultMessage[Q any] struct {
	Err    error
	Result Q
}
