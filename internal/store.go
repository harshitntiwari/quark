package internal

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DBTX interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type InsertParams struct {
	Queue       string
	Kind        string
	Args        json.RawMessage
	UniqueKey   *string
	Priority    int
	MaxAttempts int
	RunAt       time.Time
	Timeout     time.Duration
}

type ClaimParams struct {
	Queue         string
	Limit         int
	WorkerID      string
	LeaseDuration time.Duration
}

type Store interface {
	Insert(ctx context.Context, dt DBTX, p InsertParams) (Job, error)
	Claim(ctx context.Context, p ClaimParams) ([]Job, error)
	Complete(ctx context.Context, jobID int64, workerID string) error
	Snooze(ctx context.Context, jobID int64, workerID string, nextRunAt time.Time) error
	Retry(ctx context.Context, jobID int64, workerID string, attemptErr AttemptError, nextRunAt time.Time) error
	DeadLetter(ctx context.Context, jobID int64, workerID string, attemptErr AttemptError) error
	Heartbeat(ctx context.Context, jobID int64, workerID string, leaseDuration time.Duration) error
	ReapExpiredLeases(ctx context.Context) (int64, error)
	DeleteCompletedJobs(ctx context.Context, olderThan time.Time) (int64, error)
}