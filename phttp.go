package workqueue

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/thisguycodes/workqueue/queue"
)

// ReqID is a unique (per Requestor) request id
type ReqID uint64

// Span is a range of pagination we can pre-generate the requests for
type Span struct {
	Start interface{}
	End   interface{}
	Meta  interface{}
}

// Request is a request to be performed
type Request interface{}

// Doer performs requests
type Doer interface {
	ProcessRequest(context.Context, *Requestor, MarkedReq)
	ProcessSpan(context.Context, *Requestor, Span)
}

// MarkedReq is a request bundled with its ID
type MarkedReq struct {
	ID  ReqID
	Req Request
}

// NewRequestor is the Requestor constructor
func NewRequestor(ctx context.Context, doer Doer, numWorkers int) *Requestor {
	var i uint64
	req := &Requestor{
		ctx:      ctx,
		doer:     doer,
		spans:    new(sync.Map),
		wg:       new(sync.WaitGroup),
		seq:      &i,
		reqQueue: queue.New(ctx),
	}
	for i := 0; i < numWorkers; i++ {
		go req.run()
	}
	return req
}

// Requestor is an actor for performing massively parallel requests
type Requestor struct {
	ctx      context.Context
	doer     Doer
	spans    *sync.Map
	wg       *sync.WaitGroup
	seq      *uint64
	reqQueue *queue.Queue
}

// run is a worker for the queue
func (r *Requestor) run() {
	for {
		reqInt := r.reqQueue.Pop()
		if reqInt == nil {
			// a nil response means the queue context is cancelled
			break
		}
		req := reqInt.(MarkedReq)
		func() {
			defer r.wg.Done()
			r.doer.ProcessRequest(r.ctx, r, req)
		}()
	}
}

// CreateSpan creates a new span for the requestor to process.
// This is idempotent, if you attempt to create the same span multiple times
// subsequent ones will be no-ops.
//
// Span creation is run synchronously to keep the runner-count consistent.
func (r *Requestor) CreateSpan(s Span) {
	_, alreadyThere := r.spans.LoadOrStore(s, struct{}{})
	if !alreadyThere {
		r.wg.Add(1)
		func() {
			defer r.wg.Done()
			r.doer.ProcessSpan(r.ctx, r, s)
		}()
	}
}

// Add adds a request to be processed
func (r *Requestor) Add(req Request) ReqID {
	id := ReqID(atomic.AddUint64(r.seq, 1))
	r.wg.Add(1)
	r.reqQueue.Push(MarkedReq{
		ID:  id,
		Req: req,
	})
	return id
}

// Flush waits for processing to complete
func (r *Requestor) Flush() {
	r.wg.Wait()
}
