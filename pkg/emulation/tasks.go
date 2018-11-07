package emulation

import (
	"context"
	"math/rand"
	"time"

	"github.com/irvinlim/mesosmock/pkg/config"
	"github.com/irvinlim/mesosmock/pkg/state"
	"github.com/mesos/mesos-go/api/v1/lib"
)

type TaskEmulation struct {
	opts        *config.EmulationOptions
	masterState *state.MasterState
	delayQueue  *DelayQueue

	CreateTask chan *mesos.Task
	GetStatus  chan mesos.TaskStatus
}

func NewTaskEmulation(opts *config.EmulationOptions, masterState *state.MasterState) *TaskEmulation {
	return &TaskEmulation{
		opts:        opts,
		masterState: masterState,
		delayQueue:  NewDelayQueue(),
		CreateTask:  make(chan *mesos.Task),
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

		case task, ok := <-e.CreateTask:
			if !ok {
				break
			}

			event := Event{
				Deadline: time.Now().Add(time.Duration(e.opts.TaskState.DelayTaskStaging * float64(time.Second))),
				Task: &EventTask{
					Task:  task,
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
			if nextEvent := e.getNextEvent(task); nextEvent != nil {
				send <- nextEvent
			}
		case <-time.After(1 * time.Second):
		}
	}
}

func (e *TaskEmulation) getNextEvent(task *EventTask) *Event {
	opts := e.opts.TaskState

	stagingWeights := make(map[float64]mesos.TaskState)
	stagingWeights[opts.RatioTaskError] = mesos.TASK_ERROR
	stagingWeights[1-opts.RatioTaskError] = mesos.TASK_STARTING

	startingWeights := make(map[float64]mesos.TaskState)
	startingWeights[opts.RatioTaskDropped] = mesos.TASK_DROPPED
	startingWeights[opts.RatioTaskFailed] = mesos.TASK_FAILED
	startingWeights[opts.RatioTaskFinished] = mesos.TASK_FINISHED
	startingWeights[opts.RatioTaskUnreachable] = mesos.TASK_UNREACHABLE
	startingWeights[opts.RatioTaskGone] = mesos.TASK_GONE
	startingWeights[opts.RatioTaskGoneByOperator] = mesos.TASK_GONE_BY_OPERATOR

	nextState := mesos.TASK_UNKNOWN
	delay := opts.DelayTaskNextState

	// TASK_STAGING -> TASK_STARTING | TASK_ERROR
	if task.State == mesos.TASK_STAGING {
		nextState = weightedRandomSelect(stagingWeights)
		if nextState == mesos.TASK_STARTING {
			delay = opts.DelayTaskStarting
		}
	}

	// TASK_STARTING -> TASK_XXX
	if task.State == mesos.TASK_STARTING {
		nextState = weightedRandomSelect(startingWeights)
	}

	// TASK_RUNNING -> TASK_XXX
	if task.State == mesos.TASK_RUNNING {
		nextState = weightedRandomSelect(startingWeights)
	}

	// TASK_LOST -> TASK_RUNNING
	if task.State == mesos.TASK_LOST {
		if rand.Float64() < opts.RatioTaskLostRecovered {
			nextState = mesos.TASK_RUNNING
			delay = opts.DelayTaskLostRecovered
		}
	}

	if nextState != mesos.TASK_UNKNOWN {
		return createEvent(task.Task, nextState, delay)
	}

	return nil
}

func createEvent(task *mesos.Task, state mesos.TaskState, delay float64) *Event {
	event := Event{
		Deadline: time.Now().Add(time.Duration(delay * float64(time.Second))),
		Task: &EventTask{
			Task:  task,
			State: state,
		},
	}

	// TODO: Emulate task health.
	if state == mesos.TASK_RUNNING {
		healthy := true
		event.Task.Healthy = &healthy
	}

	return &event
}

func createStatus(e *EventTask) mesos.TaskStatus {
	if e == nil {
		panic("event task cannot be nil")
	}

	timestamp := float64(time.Now().UnixNano()) / float64(time.Second)
	return mesos.TaskStatus{
		TaskID:    e.Task.TaskID,
		AgentID:   &e.Task.AgentID,
		Healthy:   e.Healthy,
		State:     &e.State,
		Timestamp: &timestamp,
	}
}
