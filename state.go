package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/mesos/mesos-go/api/v1/lib"
)

// MasterState stores any global information about the mock Mesos cluster and master.
type MasterState struct {
	MasterInfo *mesos.MasterInfo

	// Frameworks registered in the master.
	Frameworks map[mesos.FrameworkID]*FrameworkState

	// Tasks that are created and not in terminal state.
	Tasks map[mesos.TaskID]mesos.Task

	// Offers sent to frameworks that are not yet accepted or declined.
	Offers map[mesos.OfferID]mesos.Offer

	// AgentIDs store a list of generated agent IDs.
	AgentIDs []mesos.AgentID
}

// FrameworkState stores any global information about a single framework registered on the master.
type FrameworkState struct {
	FrameworkInfo *mesos.FrameworkInfo

	offerOffset       int
	outstandingOffers map[mesos.AgentID]mesos.OfferID
}

// NewMasterState initialises a new master state for the mock cluster.
func NewMasterState(opts *Options) (*MasterState, error) {
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

		Frameworks: make(map[mesos.FrameworkID]*FrameworkState),
		Tasks:      make(map[mesos.TaskID]mesos.Task),
		Offers:     make(map[mesos.OfferID]mesos.Offer),
		AgentIDs:   generateAgents(masterID, opts.AgentCount),
	}

	return state, nil
}

// NewFramework creates and adds a new framework to the master.
func (s MasterState) NewFramework(info *mesos.FrameworkInfo) *FrameworkState {
	framework := &FrameworkState{
		FrameworkInfo:     info,
		outstandingOffers: make(map[mesos.AgentID]mesos.OfferID),
		offerOffset:       1,
	}

	s.Frameworks[*info.ID] = framework
	return framework
}

// DisconnectFramework handles disconnections of a framework from the master.
func (s MasterState) DisconnectFramework(frameworkID mesos.FrameworkID) {
	framework, exists := s.Frameworks[frameworkID]
	if !exists {
		return
	}

	// Remove all outstanding offers for the framework.
	for agentID := range framework.outstandingOffers {
		delete(framework.outstandingOffers, agentID)
	}
}

// NewOffer attempts to create a new resource offer for a framework from an agent.
// If there is an outstanding offer for the same agent + framework, this method returns nil.
func (s MasterState) NewOffer(frameworkID mesos.FrameworkID, agentID mesos.AgentID) *mesos.Offer {
	frameworkState := s.Frameworks[frameworkID]

	// For simplicity, we assume that every agent only sends one offer each time for all of its (infinite) resources.
	if _, exists := frameworkState.outstandingOffers[agentID]; exists {
		return nil
	}

	// Create new offer ID.
	offerIDString := fmt.Sprintf("%s-O%d", frameworkID.Value, frameworkState.offerOffset)
	offerID := mesos.OfferID{Value: offerIDString}

	// Add offer as outstanding offer for agent to this framework.
	frameworkState.outstandingOffers[agentID] = offerID

	// Increment offer offset for framework.
	frameworkState.offerOffset += 1

	offer := mesos.Offer{
		ID:          offerID,
		AgentID:     agentID,
		FrameworkID: frameworkID,
	}

	s.Offers[offerID] = offer
	return &offer
}

// RemoveOffer removes an existing offer, in response to the offer being accepted, declined or rescinded.
func (s MasterState) RemoveOffer(frameworkID mesos.FrameworkID, offerID mesos.OfferID) {
	if offer, exists := s.Offers[offerID]; exists {
		delete(s.Frameworks[frameworkID].outstandingOffers, offer.AgentID)
		delete(s.Offers, offerID)
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
