package internal

import (
	"encoding/json"
	"time"
)

type AttemptError struct {
	Attempt  int       `json:"attempt"`
	Error    string    `json:"error"`
	FailedAt time.Time `json:"failed_at"`
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

type MisfirePolicy string

const (
    MisfireSkip    MisfirePolicy = "skip"
    MisfireRunOnce MisfirePolicy = "run_once"
    MisfireCatchUp MisfirePolicy = "catch_up"
)

type Schedule struct {
    Name          string          `json:"name"`
    CronExpr      string          `json:"cron_expr"`
    Queue         string          `json:"queue"`
    Kind          string          `json:"kind"`
    Args          json.RawMessage `json:"args"`
    Priority      int             `json:"priority"`
    MaxAttempts   int             `json:"max_attempts"`
    Timeout       time.Duration   `json:"timeout"`
    MisfirePolicy MisfirePolicy   `json:"misfire_policy"`
    NextRunAt     time.Time       `json:"next_run_at"`
    LastRunAt     *time.Time      `json:"last_run_at"`
    Enabled       bool            `json:"enabled"`
    CreatedAt     time.Time       `json:"created_at"`
    UpdatedAt     time.Time       `json:"updated_at"`
}
