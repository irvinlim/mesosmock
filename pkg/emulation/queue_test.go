package emulation

import (
	"container/heap"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/stretchr/testify/assert"
)

func TestDelayQueue(t *testing.T) {
	q := NewDelayQueue()

	assert.Equal(t, q.Len(), 0)

	taskID := mesos.TaskID{Value: uuid.New().String()}
	healthy := true
	event := Event{
		Deadline: time.Now().Add(-1 * time.Second),
		Task: &EventTask{
			Task: &mesos.Task{
				TaskID: taskID,
			},
			State:   mesos.TASK_RUNNING,
			Healthy: &healthy,
		},
	}

	heap.Push(q, &event)
	assert.Equal(t, q.Len(), 1)

	popped := heap.Pop(q).(*Event)
	assert.NotNil(t, popped)
	assert.Equal(t, event, *popped)
	assert.Equal(t, event.Task.Task.TaskID, taskID)
	assert.Equal(t, event.Task.State, mesos.TASK_RUNNING)

	assert.Equal(t, q.Len(), 0)
}

func TestDelayQueue_Future(t *testing.T) {
	q := NewDelayQueue()

	taskID := mesos.TaskID{Value: uuid.New().String()}
	healthy := true
	event := Event{
		Deadline: time.Now().Add(100 * time.Millisecond),
		Task: &EventTask{
			Task: &mesos.Task{
				TaskID: taskID,
			},
			State:   mesos.TASK_RUNNING,
			Healthy: &healthy,
		},
	}

	heap.Push(q, &event)
	assert.Equal(t, q.Len(), 1)

	popped := heap.Pop(q)
	assert.Nil(t, popped)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, q.Len(), 1)
	newPopped := heap.Pop(q).(*Event)
	assert.NotNil(t, newPopped)
	assert.Equal(t, event, *newPopped)
}
