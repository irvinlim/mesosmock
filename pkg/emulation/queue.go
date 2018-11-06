package emulation

import (
	"container/heap"
	"context"
	"time"

	"github.com/mesos/mesos-go/api/v1/lib"
)

type DelayQueue []*Event

type Event struct {
	Deadline time.Time
	Task     *EventTask
}

type EventTask struct {
	Task    *mesos.Task
	State   mesos.TaskState
	Healthy *bool
}

func NewDelayQueue() *DelayQueue {
	q := &DelayQueue{}
	heap.Init(q)
	return q
}

func (q DelayQueue) Len() int {
	return len(q)
}

func (q DelayQueue) Less(i, j int) bool {
	// Min-heap, indexed by deadline.
	return (*q[i]).Deadline.Before((*q[j]).Deadline)
}

func (q DelayQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *DelayQueue) Push(x interface{}) {
	*q = append(*q, x.(*Event))
}

func (q *DelayQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}

func (q *DelayQueue) Peek() *Event {
	if q.Len() == 0 {
		return nil
	}
	return (*q)[0]
}

// See https://stackoverflow.com/questions/31060023/go-wait-for-next-item-in-a-priority-queue-if-empty
func (q *DelayQueue) Start(in <-chan *Event, out chan<- *Event) {
	defer close(out)

	for {
		ctx := context.Background()
		if q.Peek() != nil {
			ctx, _ = context.WithDeadline(ctx, q.Peek().Deadline)
		}

		select {
		case event := <-in:
			heap.Push(q, event)
		case <-ctx.Done():
			out <- heap.Pop(q).(*Event)
		}
	}
}
