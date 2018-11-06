package config

import "fmt"

type EmulationOptions struct {
	TaskState *TaskStateOptions `toml:"task_state"`
}

type TaskStateOptions struct {
	DelayTaskStaging        float64 `toml:"delay_task_staging"`
	DelayTaskStarting       float64 `toml:"delay_task_starting"`
	DelayTaskNextState      float64 `toml:"delay_task_next_state"`
	RatioTaskFinished       float64
	RatioTaskFailed         float64 `toml:"ratio_task_failed"`
	RatioTaskError          float64 `toml:"ratio_task_error"`
	RatioTaskDropped        float64 `toml:"ratio_task_dropped"`
	RatioTaskGone           float64 `toml:"ratio_task_gone"`
	RatioTaskGoneByOperator float64 `toml:"ratio_task_gone_by_operator"`
	RatioTaskUnreachable    float64 `toml:"ratio_task_unreachable"`
	RatioTaskLost           float64 `toml:"ratio_task_lost"`
	RatioTaskLostRecovered  float64 `toml:"ratio_task_lost_recovered"`
	DelayTaskLostRecovered  float64 `toml:"delay_task_lost_recovered"`
}

func newEmulationOptions() *EmulationOptions {
	return &EmulationOptions{
		TaskState: &TaskStateOptions{
			DelayTaskStaging:        0,
			DelayTaskStarting:       1,
			DelayTaskNextState:      5,
			RatioTaskFailed:         0,
			RatioTaskError:          0,
			RatioTaskDropped:        0,
			RatioTaskGone:           0,
			RatioTaskGoneByOperator: 0,
			RatioTaskUnreachable:    0,
			RatioTaskLost:           0,
			RatioTaskLostRecovered:  0,
			DelayTaskLostRecovered:  10,
		},
	}
}

func (o *EmulationOptions) Validate() error {
	t := o.TaskState

	ratiosToSum := []float64{t.RatioTaskDropped, t.RatioTaskFailed, t.RatioTaskGone, t.RatioTaskGoneByOperator,
		t.RatioTaskLost, t.RatioTaskUnreachable}
	otherRatios := []float64{t.RatioTaskError, t.RatioTaskLostRecovered}

	var sumRatios float64 = 0
	for _, ratio := range ratiosToSum {
		if ratio < 0 {
			return fmt.Errorf("ratio cannot be negative: %f", ratio)
		}
		sumRatios += ratio
	}

	for _, ratio := range otherRatios {
		if ratio < 0 {
			return fmt.Errorf("ratio cannot be negative: %f", ratio)
		}
	}

	// Sum of ratios cannot exceed 1.
	if sumRatios > 1 {
		return fmt.Errorf("sum of ratios cannot exceed 1")
	}

	// Ratio of TASK_FINISHED is the remaining proportion of ratios.
	t.RatioTaskFinished = 1 - sumRatios

	// Delays cannot be negative.
	delays := []float64{t.DelayTaskNextState, t.DelayTaskLostRecovered, t.DelayTaskStaging, t.DelayTaskStarting}
	for _, delay := range delays {
		if delay < 0 {
			return fmt.Errorf("delay cannot be negative: %f", delay)
		}
	}

	return nil
}
