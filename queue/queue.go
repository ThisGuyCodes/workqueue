package queue

import (
	"container/list"
	"context"
	"sync"
)

// Queue is a fifo queue, it is safe for concurrent use.
// The zero Queue is not usable, use the constructor.
type Queue struct {
	ctx   context.Context
	l     *list.List
	in    chan<- interface{}
	out   <-chan interface{}
	lenWg *sync.WaitGroup
}

// New constructs a new Queue.
// When the provided context is cancelled, push will become a no-op, pop will
// be permitted to drain the queue.
// Queues cannot be re-used once the context is cancelled.
func New(ctx context.Context) *Queue {
	in := make(chan interface{})
	out := make(chan interface{})

	q := &Queue{
		ctx:   ctx,
		l:     new(list.List),
		in:    in,
		out:   out,
		lenWg: new(sync.WaitGroup),
	}
	go q.run(in, out)
	return q
}

func (q *Queue) run(in <-chan interface{}, rawout chan<- interface{}) {
	// Definitionally when the queue is cancelled and empty .Pop()'s return
	// nil.
	// This is accomplished by closing our output channel.
	defer close(rawout)

	ctxDone := q.ctx.Done()
	isCanceled := false

	// We need to be able to disable sending when we have nothing to send.
	var out chan<- interface{}

	for {

		// if there's nothing in the list
		if q.l.Front() == nil {
			// if the context has already been cancelled
			if isCanceled {
				// we're done!
				break
			}
			// if there's nothing there, don't attempt to send it
			out = nil
		} else {
			// we have something to send, (re)enable sending
			out = rawout
		}

		select {
		case item := <-in:
			q.l.PushBack(item)
			q.lenWg.Add(1)
		case out <- q.l.Front().Value:
			q.l.Remove(q.l.Front())
			q.lenWg.Done()
		case <-ctxDone:
			// allow us to finish sending items
			ctxDone = nil
			isCanceled = true
			// stop accepting new items
			in = nil
		}
	}
}

// Pop returns the next item in the queue.
// It blocks until an item is available.
//
// Returns nil if the queue's context is cancelled and the queue is empty.
func (q *Queue) Pop() interface{} {
	return <-q.out
}

// Push adds an item to the queue.
// It no-ops when the context is expired.
func (q *Queue) Push(val interface{}) {
	select {
	case q.in <- val:
	case <-q.ctx.Done():
	}
}

// Flush waits until the queue is empty.
func (q *Queue) Flush() {
	q.lenWg.Wait()
}
