package emulation

import (
	"context"
	"time"

	"github.com/google/uuid"
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
	in := make(chan *Event)
	out := make(chan *Event)
	quit := make(chan bool)
	defer func() { quit <- true }()

	go e.delayQueue.Start(in, out, quit)
	go e.consume(ctx, in, out, frameworkID)
	go e.produce(ctx, in, frameworkID)

	<-ctx.Done()
}

func (e *TaskEmulation) produce(ctx context.Context, send chan<- *Event, frameworkID mesos.FrameworkID) {
	defer close(send)

	for {
		select {
		case <-ctx.Done():
			return

		// TODO: Temporary placeholder. Consume from e.CreateTask instead.
		case <-time.After(1 * time.Second):
			task := mesos.Task{
				TaskID:  mesos.TaskID{Value: uuid.New().String()},
				AgentID: mesos.AgentID{Value: uuid.New().String()},
			}

			event := Event{
				Deadline: time.Now().Add(time.Duration(e.opts.TaskState.DelayTaskStaging * float64(time.Second))),
				Task: &EventTask{
					Task:  &task,
					State: mesos.TASK_STAGING,
				},
			}

			send <- &event
		}
	}
}

func (e *TaskEmulation) consume(ctx context.Context, send chan<- *Event, recv <-chan *Event, frameworkID mesos.FrameworkID) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-recv:
			if !ok {
				break
			}

			task := event.Task

			// Send status update
			e.GetStatus <- createStatus(task)

			// Handle status update and emulate next event
			// TODO: Implement actual emulation, currently all tasks transit to error
			if task.State != mesos.TASK_ERROR {
				send <- &Event{
					Deadline: time.Now().Add(5 * time.Second),
					Task: &EventTask{
						Task:  task.Task,
						State: mesos.TASK_ERROR,
					},
				}
			}
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
