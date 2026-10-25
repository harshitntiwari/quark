package internal

import (
	"context"
	"encoding/json"
	"errors"
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
	Retry(ctx context.Context, jobID int64, workerID string, attemptErr AttemptError, nextRunAt time.Time) error
	DeadLetter(ctx context.Context, jobID int64, workerID string, attemptErr AttemptError) error
}

var ErrDuplicateJob = errors.New("job already exists for this unique_key")
var ErrLeaseLost = errors.New("lease lost by worker")