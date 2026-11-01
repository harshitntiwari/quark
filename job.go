package quark

import (
	"time"

	"github.com/harshitntiwari/quark/internal"
)

type JobArgs interface {
	Kind() string
}

type JobState = internal.JobState

const (
	Available JobState = internal.Available
	Running   JobState = internal.Running
	Completed JobState = internal.Completed
	Dead      JobState = internal.Dead
	Cancelled JobState = internal.Cancelled
)

type Job = internal.Job
type AttemptError = internal.AttemptError

type NonRetryableError = internal.NonRetryableError
type SnoozeError = internal.SnoozeError

func NonRetryable(err error) error {
    return &NonRetryableError{Err: err}
}

func Snooze(d time.Duration) error {
    return &SnoozeError{Duration: d}
}