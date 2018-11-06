package state

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/irvinlim/mesosmock/pkg/config"
	"github.com/mesos/mesos-go/api/v1/lib"
	"github.com/mesos/mesos-go/api/v1/lib/master"
	log "github.com/sirupsen/logrus"
)

// MasterState stores any global information about the mock Mesos cluster and master.
type MasterState struct {
	MasterInfo *mesos.MasterInfo

	// Frameworks registered in the master.
	Frameworks *sync.Map

	// Offers sent to frameworks that are not yet accepted or declined.
	Offers *sync.Map

	// Agents store a list of generated Mesos agents.
	Agents *sync.Map

	agentOffset *uint64
}

// FrameworkState stores any global information about a single framework registered on the master.
type FrameworkState struct {
	FrameworkInfo *mesos.FrameworkInfo

	// Tasks for the framework that are created and not in terminal state.
	NonTerminalTasks *sync.Map
	TerminalTasks    *sync.Map

	offerOffset       *uint64
	outstandingOffers *sync.Map
	refusedAgents     *sync.Map
}

// AgentState stores information about a single agent registered on the master.
type AgentState = master.Response_GetAgents_Agent

type refusedAgent struct {
	mesos.AgentID
	expiry time.Time
}

// NewMasterState initialises a new master state for the mock cluster.
func NewMasterState(opts *config.Options) (*MasterState, error) {
	var agentOffset uint64 = 0
	masterID := uuid.New().String()

	state := &MasterState{
		MasterInfo: &mesos.MasterInfo{
			ID: masterID,
			Address: &mesos.Address{
				Port:     int32(opts.Port),
				IP:       &opts.IP,
				Hostname: &opts.Hostname,
			},
		},

		Frameworks: new(sync.Map),
		Offers:     new(sync.Map),
		Agents:     new(sync.Map),

		agentOffset: &agentOffset,
	}

	if err := state.initState(opts); err != nil {
		return nil, err
	}

	return state, nil
}

func (s MasterState) initState(opts *config.Options) error {
	// Generate an initial number of agents.
	for i := 0; i < opts.Mesos.AgentCount; i++ {
		agent := s.NewAgent()
		log.Debugf("Created new agent %s", agent.AgentInfo.ID.Value)
	}

	return nil
}

// NewFramework creates and adds a new framework to the master.
func (s MasterState) NewFramework(info *mesos.FrameworkInfo) *FrameworkState {
	var offerOffset uint64 = 0

	framework := &FrameworkState{
		FrameworkInfo:     info,
		NonTerminalTasks:  new(sync.Map),
		TerminalTasks:     new(sync.Map),
		offerOffset:       &offerOffset,
		outstandingOffers: new(sync.Map),
		refusedAgents:     new(sync.Map),
	}

	s.Frameworks.Store(*info.ID, framework)
	return framework
}

// DisconnectFramework handles disconnections of a framework from the master.
func (s MasterState) DisconnectFramework(frameworkID mesos.FrameworkID) {
	framework, exists := s.GetFramework(frameworkID)
	if !exists {
		return
	}

	// Remove all outstanding offers for the framework.
	framework.outstandingOffers.Range(func(agentID interface{}, _ interface{}) bool {
		framework.outstandingOffers.Delete(agentID)
		return true
	})
}

// NewOffer attempts to create a new resource offer for a framework from an agent.
// If there is an outstanding offer for the same agent + framework, this method returns nil.
func (s MasterState) NewOffer(frameworkID mesos.FrameworkID, agentID mesos.AgentID) *mesos.Offer {
	frameworkState, exists := s.GetFramework(frameworkID)
	if !exists {
		return nil
	}

	// For simplicity, we assume that every agent only sends one offer each time for all of its (infinite) resources.
	if _, exists := frameworkState.outstandingOffers.Load(agentID); exists {
		return nil
	}

	// Do not create a new offer if agent is refused within the specified duration.
	if r, exists := frameworkState.refusedAgents.Load(agentID); exists {
		if !r.(refusedAgent).expiry.Before(time.Now()) {
			return nil
		}
		frameworkState.refusedAgents.Delete(agentID)
	}

	// Create new offer ID.
	offset := atomic.LoadUint64(frameworkState.offerOffset)
	offerIDString := fmt.Sprintf("%s-O%d", frameworkID.Value, offset)
	offerID := mesos.OfferID{Value: offerIDString}

	// Add offer as outstanding offer for agent to this framework.
	frameworkState.outstandingOffers.Store(agentID, offerID)

	// Increment offer offset for framework.
	atomic.AddUint64(frameworkState.offerOffset, 1)

	offer := &mesos.Offer{
		ID:          offerID,
		AgentID:     agentID,
		FrameworkID: frameworkID,
	}

	s.Offers.Store(offerID, offer)
	return offer
}

// RemoveOffer removes an existing offer, in response to the offer being accepted, declined or rescinded.
func (s MasterState) RemoveOffer(frameworkID mesos.FrameworkID, offerID mesos.OfferID, refuseDuration time.Duration) {
	if offer, exists := s.GetOffer(offerID); exists {
		if framework, exists := s.GetFramework(frameworkID); exists {
			framework.outstandingOffers.Delete(offer.AgentID)
			s.Offers.Delete(offerID)

			refusedAgent := refusedAgent{offer.AgentID, time.Now().Add(refuseDuration)}
			framework.refusedAgents.Store(offer.AgentID, refusedAgent)
		}
	}
}

func (s MasterState) NewAgent() *AgentState {
	// Atomically increment counter, actual value is before increment
	offset := atomic.AddUint64(s.agentOffset, 1)

	agentID := mesos.AgentID{Value: fmt.Sprintf("%s-S%d", s.MasterInfo.ID, offset-1)}
	port := int32(5051)
	pid := fmt.Sprintf("slave(1)@%s:%d", *s.MasterInfo.Address.IP, port)

	agent := &AgentState{
		AgentInfo: mesos.AgentInfo{
			ID:       &agentID,
			Port:     &port,
			Hostname: fmt.Sprintf("mesos-slave-%d", offset-1),
		},
		PID:    &pid,
		Active: true,
	}

	s.Agents.Store(agentID, agent)
	return agent
}

func (s MasterState) NewTask(frameworkID mesos.FrameworkID, taskInfo mesos.TaskInfo) *mesos.Task {
	framework, exists := s.GetFramework(frameworkID)
	if !exists {
		return nil
	}

	// We ignore command and healthcheck in the TaskInfo as it is not relevant to our needs right now.
	state := mesos.TASK_STAGING
	task := &mesos.Task{
		Name:        taskInfo.Name,
		TaskID:      taskInfo.TaskID,
		AgentID:     taskInfo.AgentID,
		FrameworkID: frameworkID,
		State:       &state,
		Container:   taskInfo.Container,
		Labels:      taskInfo.Labels,
		Resources:   taskInfo.Resources,
		ExecutorID:  &taskInfo.Executor.ExecutorID,
		Discovery:   taskInfo.Discovery,
	}

	framework.NonTerminalTasks.Store(taskInfo.TaskID, &task)
	return task
}

func (s MasterState) GetFramework(frameworkID mesos.FrameworkID) (*FrameworkState, bool) {
	framework, exists := s.Frameworks.Load(frameworkID)
	if !exists {
		return nil, false
	}

	return framework.(*FrameworkState), true
}

func (s MasterState) GetTask(frameworkID mesos.FrameworkID, taskID mesos.TaskID) (*mesos.Task, bool) {
	framework, exists := s.GetFramework(frameworkID)
	if !exists {
		return nil, false
	}

	task, exists := framework.NonTerminalTasks.Load(taskID)
	if !exists {
		task, exists = framework.TerminalTasks.Load(taskID)
	}
	if !exists {
		return nil, false
	}

	return task.(*mesos.Task), true
}

func (s MasterState) GetTasks() []mesos.Task {
	var tasks []mesos.Task

	appendTask := func(taskID interface{}, task interface{}) bool {
		tasks = append(tasks, *task.(*mesos.Task))
		return true
	}

	s.Frameworks.Range(func(frameworkID interface{}, framework interface{}) bool {
		f := framework.(*FrameworkState)
		f.NonTerminalTasks.Range(appendTask)
		f.TerminalTasks.Range(appendTask)
		return true
	})

	return tasks
}

func (s MasterState) GetOffer(offerID mesos.OfferID) (*mesos.Offer, bool) {
	offer, exists := s.Offers.Load(offerID)
	if !exists {
		return nil, false
	}

	return offer.(*mesos.Offer), true
}

func (s MasterState) GetAgents() []AgentState {
	var agents []AgentState
	s.Agents.Range(func(agentID interface{}, agent interface{}) bool {
		agents = append(agents, *agent.(*AgentState))
		return true
	})

	return agents
}

func (s MasterState) GetAgentIDs() []mesos.AgentID {
	var agents []mesos.AgentID
	s.Agents.Range(func(agentID interface{}, agent interface{}) bool {
		agents = append(agents, *agent.(*AgentState).AgentInfo.ID)
		return true
	})

	return agents
}
