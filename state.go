package main

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/irvinlim/mesosmock/pkg/config"
	"github.com/mesos/mesos-go/api/v1/lib"
)

// MasterState stores any global information about the mock Mesos cluster and master.
type MasterState struct {
	MasterInfo *mesos.MasterInfo

	// Frameworks registered in the master.
	Frameworks *sync.Map

	// Tasks that are created and not in terminal state.
	Tasks *sync.Map

	// Offers sent to frameworks that are not yet accepted or declined.
	Offers *sync.Map

	// AgentIDs store a list of generated agent IDs.
	AgentIDs []mesos.AgentID
}

// FrameworkState stores any global information about a single framework registered on the master.
type FrameworkState struct {
	FrameworkInfo *mesos.FrameworkInfo

	offerOffset       int64
	outstandingOffers *sync.Map
}

// NewMasterState initialises a new master state for the mock cluster.
func NewMasterState(opts *config.Options) (*MasterState, error) {
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
		Tasks:      new(sync.Map),
		Offers:     new(sync.Map),
		AgentIDs:   generateAgents(masterID, opts.AgentCount),
	}

	return state, nil
}

// NewFramework creates and adds a new framework to the master.
func (s MasterState) NewFramework(info *mesos.FrameworkInfo) *FrameworkState {
	framework := &FrameworkState{
		FrameworkInfo:     info,
		outstandingOffers: new(sync.Map),
		offerOffset:       1,
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

	// Create new offer ID.
	offerIDString := fmt.Sprintf("%s-O%d", frameworkID.Value, frameworkState.offerOffset)
	offerID := mesos.OfferID{Value: offerIDString}

	// Add offer as outstanding offer for agent to this framework.
	frameworkState.outstandingOffers.Store(agentID, offerID)

	// Increment offer offset for framework.
	atomic.AddInt64(&frameworkState.offerOffset, 1)

	offer := &mesos.Offer{
		ID:          offerID,
		AgentID:     agentID,
		FrameworkID: frameworkID,
	}

	s.Offers.Store(offerID, offer)
	return offer
}

// RemoveOffer removes an existing offer, in response to the offer being accepted, declined or rescinded.
func (s MasterState) RemoveOffer(frameworkID mesos.FrameworkID, offerID mesos.OfferID) {
	if offer, exists := s.GetOffer(offerID); exists {
		if framework, exists := s.GetFramework(frameworkID); exists {
			framework.outstandingOffers.Delete(offer.AgentID)
			s.Offers.Delete(offerID)
		}
	}
}

func generateAgents(masterID string, agentCount int) []mesos.AgentID {
	var agentIDs []mesos.AgentID
	for i := 0; i < agentCount; i++ {
		agentIDs = append(agentIDs, mesos.AgentID{
			Value: fmt.Sprintf("%s-S%d", masterID, i),
		})
	}

	return agentIDs
}

func (s MasterState) GetFramework(frameworkID mesos.FrameworkID) (*FrameworkState, bool) {
	framework, exists := s.Frameworks.Load(frameworkID)
	if !exists {
		return nil, false
	}

	return framework.(*FrameworkState), true
}

func (s MasterState) GetTask(taskID mesos.TaskID) (*mesos.Task, bool) {
	task, exists := s.Tasks.Load(taskID)
	if !exists {
		return nil, false
	}

	return task.(*mesos.Task), true
}

func (s MasterState) GetTasks() []mesos.Task {
	var tasks []mesos.Task
	s.Tasks.Range(func(taskID interface{}, task interface{}) bool {
		tasks = append(tasks, *task.(*mesos.Task))
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
