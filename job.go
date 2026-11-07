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

var (
	ErrDuplicateJob = internal.ErrDuplicateJob
	ErrLeaseLost    = internal.ErrLeaseLost
)

type Job = internal.Job
type AttemptError = internal.AttemptError

type NonRetryableError = internal.NonRetryableError
type SnoozeError = internal.SnoozeError

// NonRetryable wraps an error to signal that the job should bypass 
// remaining retry attempts and move directly to the dead state.
func NonRetryable(err error) error {
    return &NonRetryableError{Err: err}
}

// Snooze returns an error that reschedules the job to run 
// after the specified duration without consuming an attempt or recording a failure.
func Snooze(d time.Duration) error {
    return &SnoozeError{Duration: d}
}

type Schedule = internal.Schedule
type MisfirePolicy = internal.MisfirePolicy

const (
    MisfireSkip    MisfirePolicy = internal.MisfireSkip
    MisfireRunOnce MisfirePolicy = internal.MisfireRunOnce
    MisfireCatchUp MisfirePolicy = internal.MisfireCatchUp
)