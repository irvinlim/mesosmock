package emulation

import (
	"math/rand"

	"github.com/mesos/mesos-go/api/v1/lib"
)

func weightedRandomSelect(weights map[float64]mesos.TaskState) mesos.TaskState {
	sumOfWeights := 0.0
	for weight := range weights {
		sumOfWeights += weight
	}

	var lastState mesos.TaskState
	randWeight := rand.Float64() * sumOfWeights
	for weight, state := range weights {
		lastState = state
		randWeight -= weight
		if randWeight <= 0 {
			return state
		}
	}

	return lastState
}
