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
	ID          int64           `json:"id"`
	Queue       string          `json:"queue"`
	Kind        string          `json:"kind"`
	Args        json.RawMessage `json:"args"`
	UniqueKey   *string         `json:"unique_key"`
	Priority    int             `json:"priority"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	State       JobState        `json:"state"`
	RunAt       time.Time       `json:"run_at"`
	Timeout     time.Duration   `json:"timeout"`
	LockedBy    *string         `json:"locked_by"`
	LockedUntil *time.Time      `json:"locked_until"`
	Errors      []AttemptError  `json:"errors"`
	CreatedAt   time.Time       `json:"created_at"`
	FinishedAt  *time.Time      `json:"finished_at"`
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
