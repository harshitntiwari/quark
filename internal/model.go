package internal

import (
	"encoding/json"
	"time"
)

type AttemptError struct {
	Attempt  int
	Error    string
	FailedAt time.Time
}

type Job struct {
	ID          int64
	Queue       string
	Kind        string
	Args        json.RawMessage
	UniqueKey   *string
	Priority    int
	Attempts    int
	MaxAttempts int
	State       JobState
	RunAt       time.Time
	Timeout     time.Duration
	LockedBy    *string
	LockedUntil *time.Time
	Errors      []AttemptError
	CreatedAt   time.Time
	FinishedAt  *time.Time
}
