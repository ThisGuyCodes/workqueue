package workqueue

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/thisguycodes/workqueue/queue"
)

// ItemID is a unique (per WorkQueue) WorkItem ID.
type ItemID uint64

// WorkItem is a unit of work to be performed.
type WorkItem interface{}

// Worker is the thing that does the work in a WorkQueue.
type Worker interface {
	// Work must be safe for concurrent use.
	Work(context.Context, *WorkQueue, MarkedWorkItem)
}

// MarkedWorkItem is a WorkItem bundled with its ID.
type MarkedWorkItem struct {
	ID  ItemID
	Req WorkItem
}

// New is the WorkQueue constructor.
func New(ctx context.Context, doer Worker, numWorkers int) *WorkQueue {
	var i uint64
	req := &WorkQueue{
		ctx:      ctx,
		doer:     doer,
		seq:      i,
		reqQueue: queue.New(ctx),
	}
	for i := 0; i < numWorkers; i++ {
		go req.run()
	}
	return req
}

// WorkQueue is an actor for performing parallel work on a FIFO queue.
type WorkQueue struct {
	ctx      context.Context
	doer     Worker
	wg       sync.WaitGroup
	seq      uint64
	reqQueue *queue.Queue
}

// run is a worker for the queue.
func (r *WorkQueue) run() {
	for {
		reqInt := r.reqQueue.Pop()
		if reqInt == nil {
			// A nil response means the queue context is cancelled.
			break
		}
		req := reqInt.(MarkedWorkItem)

		// Protect from panics in the worker's code.
		func() {
			defer r.wg.Done()
			r.doer.Work(r.ctx, r, req)
		}()
	}
}

// Add adds a WorkItem to be processed by the WorkQueue.
func (r *WorkQueue) Add(req WorkItem) ItemID {
	id := ItemID(atomic.AddUint64(&r.seq, 1))
	pushed := r.reqQueue.Push(MarkedWorkItem{
		ID:  id,
		Req: req,
	})
	// Only add to the waitgroup if we're actually gonna do work...
	if pushed {
		r.wg.Add(1)
	}
	return id
}

// Flush waits for processing to complete.
func (r *WorkQueue) Flush() {
	r.wg.Wait()
}
