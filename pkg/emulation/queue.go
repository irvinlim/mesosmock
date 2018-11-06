package emulation

import (
	"container/heap"
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
	if (*old[0]).Deadline.After(time.Now()) {
		return nil
	}

	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}
