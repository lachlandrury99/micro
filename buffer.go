package micro

import "sync"

// Buffer is a collection utility that allows items to be collected until
// a point in which they are ready to be utilized, allowing the buffer to
// be flushed and reset.
//
// Buffer is safe for concurrent use.
type Buffer[T any] struct {
	delegate []T
	mu       sync.Mutex
}

// newBuffer returns a new empty buffer
func newBuffer[T any]() *Buffer[T] {
	return &Buffer[T]{
		delegate: make([]T, 0),
	}
}

// Add adds an item to the buffer
func (b *Buffer[T]) Add(item T) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.delegate = append(b.delegate, item)
}

// Size returns the number of items in the buffer
func (b *Buffer[T]) Size() int {
	return len(b.delegate)
}

// Flush returns the buffered items and resets the buffer
func (b *Buffer[T]) Flush() []T {
	b.mu.Lock()
	defer b.mu.Unlock()

	items := b.delegate
	b.delegate = make([]T, 0)

	return items
}
