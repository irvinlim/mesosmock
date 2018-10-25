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

	offerOffset int
}

// State is the current MasterState of the mesosmock instance.
var State *MasterState

func InitState(opts *Options) error {
	masterID := uuid.New().String()
	State = &MasterState{
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

	return nil
}

func NewFramework(info *mesos.FrameworkInfo) *FrameworkState {
	return &FrameworkState{
		FrameworkInfo: info,
		offerOffset:   1,
	}
}

func NewOffer(frameworkID mesos.FrameworkID, agentID mesos.AgentID) mesos.Offer {
	offerID := fmt.Sprintf("%s-O%d", frameworkID.Value, State.Frameworks[frameworkID].offerOffset)
	State.Frameworks[frameworkID].offerOffset += 1

	return mesos.Offer{
		ID:          mesos.OfferID{Value: offerID},
		AgentID:     agentID,
		FrameworkID: frameworkID,
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
