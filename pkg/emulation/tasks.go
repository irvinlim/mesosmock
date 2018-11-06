package emulation

import (
	"container/heap"
	"context"
	"time"

	"github.com/irvinlim/mesosmock/pkg/config"
	"github.com/irvinlim/mesosmock/pkg/state"
	"github.com/mesos/mesos-go/api/v1/lib"
)

type TaskEmulation struct {
	opts        *config.EmulationOptions
	masterState *state.MasterState
	delayQueue  *DelayQueue

	CreateTask chan mesos.Task
	GetStatus  chan mesos.TaskStatus
}

func NewTaskEmulation(opts *config.EmulationOptions, masterState *state.MasterState) *TaskEmulation {
	return &TaskEmulation{
		opts:        opts,
		masterState: masterState,
		delayQueue:  NewDelayQueue(),
		GetStatus:   make(chan mesos.TaskStatus),
	}
}

func (e *TaskEmulation) EmulateTasks(ctx context.Context, frameworkID mesos.FrameworkID) {
	go e.consume(ctx, frameworkID)
	go e.produce(ctx, frameworkID)
}

func (e *TaskEmulation) produce(ctx context.Context, frameworkID mesos.FrameworkID) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-e.CreateTask:
			event := &Event{
				Deadline: time.Now().Add(time.Duration(e.opts.TaskState.DelayTaskStaging * float64(time.Second))),
				Task: &EventTask{
					Task:  &task,
					State: mesos.TASK_STAGING,
				},
			}
			heap.Push(e.delayQueue, event)
		}
	}
}

func (e *TaskEmulation) consume(ctx context.Context, frameworkID mesos.FrameworkID) {
	for {
		// Consume from delay queue
		if e.delayQueue.Len() > 0 {
			event := heap.Pop(e.delayQueue)
			if event != nil {
				task := event.(*Event).Task

				// Send status update
				e.GetStatus <- createStatus(task)

				// Handle status update and emulate next event
				// TODO: Implement actual emulation, currently all tasks transit to error
				if task.State != mesos.TASK_ERROR {
					newEvent := &Event{
						Deadline: time.Now().Add(5 * time.Second),
						Task: &EventTask{
							Task:  task.Task,
							State: mesos.TASK_ERROR,
						},
					}
					heap.Push(e.delayQueue, newEvent)
				}
			}
		}

		// TODO: Make delay queue thread-safe with channels.
		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func createStatus(e *EventTask) mesos.TaskStatus {
	if e == nil {
		panic("event task cannot be nil")
	}

	return mesos.TaskStatus{
		TaskID:  e.Task.TaskID,
		AgentID: &e.Task.AgentID,
		Healthy: e.Healthy,
		State:   &e.State,
	}
}
