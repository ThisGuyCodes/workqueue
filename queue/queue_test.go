package queue_test

import (
	"context"
	"sync"
	"testing"

	"github.com/thisguycodes/workqueue/queue"
)

// Test that an empty + cancelled queue returns nil from Pop.
func TestQueuePopCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	q := queue.New(ctx)

	cancel()

	v := q.Pop()

	if v != nil {
		t.Errorf("Queue didn't return nil after being cancelled, got %#v", v)
	}
}

// Test that a queue is FIFO, and that the queue drains after bing cancelled.
// Also some basic concurrency validation.
// Also that pushing after cancelling no-ops.
// ...This is a big test.
func TestQueuePopInOrder(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	q := queue.New(ctx)

	const count = 10000

	wait := sync.Mutex{}
	wait.Lock()
	go func() {
		defer wait.Unlock()

		for i := 0; i < count; i++ {
			q.Push(i)
		}

		cancel()

		for i := 0; i < count; i++ {
			ret := q.Push(i)
			if ret {
				t.Errorf("Queue accepted a push after being cancelled")
			}
		}
	}()

	for i := 0; i < count; i++ {
		v := q.Pop()
		if v.(int) != i {
			t.Errorf("Queue returned things out of order, got %#v expected %#v", v, i)
		}
	}

	v := q.Pop()
	if v != nil {
		t.Errorf("Queue didn't return nil after being cancelled, got %#v", v)
	}

	// Wait for the goroutine to finish.
	wait.Lock()
}

func TestQueueFlush(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	q := queue.New(ctx)

	const count = 1000000

	for i := 0; i < count; i++ {
		q.Push(i)
	}
	cancel()

	go func() {
		for i := 0; i < count; i++ {
			q.Pop()
		}
	}()

	q.Flush()
	v := q.Pop()
	if v != nil {
		t.Errorf("Queue should be empty and cancelled after flush, got %#v", v)
	}
}
