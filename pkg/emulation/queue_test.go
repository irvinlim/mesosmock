package emulation

import (
	"container/heap"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/stretchr/testify/require"
)

func newEvent(delay time.Duration) Event {
	taskID := mesos.TaskID{Value: uuid.New().String()}
	healthy := true
	event := Event{
		Deadline: time.Now().Add(delay),
		Task: &EventTask{
			Task: &mesos.Task{
				TaskID: taskID,
			},
			State:   mesos.TASK_RUNNING,
			Healthy: &healthy,
		},
	}

	return event
}

func newQueue() (*DelayQueue, chan *Event, chan *Event, chan bool) {
	q := NewDelayQueue()
	in := make(chan *Event)
	out := make(chan *Event)
	quit := make(chan bool)
	return q, in, out, quit
}

func TestDelayQueue(t *testing.T) {
	q, _, _, _ := newQueue()
	event := newEvent(-1 * time.Second)

	require.Equal(t, q.Len(), 0)

	heap.Push(q, &event)
	require.Equal(t, q.Len(), 1)

	popped := heap.Pop(q).(*Event)
	require.NotNil(t, popped)
	require.Equal(t, event, *popped)
	require.Equal(t, event.Task.Task.TaskID, popped.Task.Task.TaskID)
	require.Equal(t, event.Task.State, mesos.TASK_RUNNING)

	require.Equal(t, q.Len(), 0)
}

func TestDelayQueue_Heap(t *testing.T) {
	q, _, _, _ := newQueue()
	var events []*Event

	for i := 9; i >= 0; i-- {
		event := newEvent(time.Duration(i * int(time.Second)))
		events = append(events, &event)
	}

	for i, event := range events {
		heap.Push(q, event)
		require.Equal(t, q.Len(), i+1)
	}

	for i := 9; i >= 0; i-- {
		popped := heap.Pop(q).(*Event)
		require.NotNil(t, popped)
		require.Equal(t, *events[i], *popped)
	}
}

func TestDelayQueue_Chan(t *testing.T) {
	q, in, out, quit := newQueue()
	event := newEvent(-1 * time.Second)

	go q.Start(in, out, quit)
	defer func() { quit <- true }()

	in <- &event

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	select {
	case <-ctx.Done():
		require.FailNow(t, "did not read from channel in time")
	case readEvent := <-out:
		require.NotNil(t, readEvent)
		require.Equal(t, event, *readEvent)
	}
}

func TestDelayQueue_ChanFuture(t *testing.T) {
	q, in, out, quit := newQueue()
	event := newEvent(100 * time.Millisecond)

	go q.Start(in, out, quit)
	defer func() { quit <- true }()

	in <- &event

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		require.FailNow(t, "did not read from channel in time")
	case readEvent := <-out:
		require.NotNil(t, readEvent)
		require.Equal(t, event, *readEvent)
	}
}

func TestDelayQueue_ChanHeap(t *testing.T) {
	q, in, out, quit := newQueue()
	var events []*Event
	for i := 0; i < 10; i++ {
		event := newEvent(time.Duration((i + 1) * 100 * int(time.Millisecond)))
		events = append(events, &event)
	}

	go q.Start(in, out, quit)
	defer func() { quit <- true }()

	go func() {
		for i := 9; i >= 0; i-- {
			in <- events[i]
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		require.FailNow(t, "did not read from channel in time")
	case readEvent := <-out:
		require.NotNil(t, readEvent)
		n := len(events)
		require.Equal(t, *events[0], *readEvent)
		events = events[1:n]
	}
}
