package internal

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DBStore struct {
	pool *pgxpool.Pool
}

// allocates and returns a new [DBStore]
func NewDBStore(pool *pgxpool.Pool) *DBStore {
	return &DBStore{pool}
}

// hydrates a raw db row from `jobs` table into a [Job] struct
func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var timeoutMS int64

	err := row.Scan(
		&j.ID, &j.Queue, &j.Kind, &j.Args, &j.UniqueKey, &j.Priority,
		&j.Attempts, &j.MaxAttempts, &j.State, &j.RunAt, &timeoutMS,
		&j.LockedBy, &j.LockedUntil, &j.Errors, &j.CreatedAt, &j.FinishedAt,
	)
	if err != nil {
		return Job{}, err
	}

	// convert timeoutMS (which is in ms) to time.Duration
	j.Timeout = time.Duration(timeoutMS) * time.Millisecond

	return j, nil
}

// inserts a new job into `jobs` table within the caller's db transaction
func (db *DBStore) Insert(ctx context.Context, dt DBTX, p InsertParams) (Job, error) {
	const query = `
		INSERT INTO jobs (queue, kind, args, unique_key, priority, max_attempts, run_at, timeout_ms) values ($1, $2, $3, $4, $5, $6, $7, $8) 
		ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL AND state IN ('available', 'running')
		DO NOTHING 
		RETURNING
			id, queue, kind, args, unique_key, priority, attempts, max_attempts,
			state, run_at, timeout_ms, locked_by, locked_until, errors, created_at, finished_at
	`

	row := dt.QueryRow(ctx, query, p.Queue, p.Kind, p.Args, p.UniqueKey, p.Priority, p.MaxAttempts, p.RunAt, p.Timeout.Milliseconds())

	j, err := scanJob(row)

	if err != nil {
		// ON CONFLICT DO NOTHING inserts 0 rows, QueryRow() returns 0 rows and Scan() returns pgx.ErrNoRows
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrDuplicateJob
		}
		return Job{}, err
	}
	return j, err
}

// moves a fixed number of jobs from 'available' state to 'running' state and returns them
func (db *DBStore) Claim(ctx context.Context, p ClaimParams) ([]Job, error) {
	const query = `
		UPDATE jobs SET state = 'running', attempts = attempts + 1, locked_by = $1, locked_until = NOW() + ($2 * INTERVAL '1 millisecond')
		WHERE id IN (
			SELECT id FROM jobs
			WHERE queue = $3 AND state = 'available' AND run_at <= NOW()
			ORDER BY priority ASC, run_at ASC, id ASC
			LIMIT $4
			FOR UPDATE SKIP LOCKED
		)
		RETURNING
			id, queue, kind, args, unique_key, priority, attempts, max_attempts,
			state, run_at, timeout_ms, locked_by, locked_until, errors, created_at, finished_at
	`

	rows, err := db.pool.Query(ctx, query, p.WorkerID, p.LeaseDuration.Milliseconds(), p.Queue, p.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return jobs, nil
}

// moves a task from 'running' state to 'completed' state
func (db *DBStore) Complete(ctx context.Context, jobID int64, workerID string) error {
	const query = `
		UPDATE jobs SET state = 'completed', finished_at = NOW(), locked_by = null, locked_until = null
		WHERE id = $1 AND locked_by = $2 AND state = 'running'
	`
	tag, err := db.pool.Exec(ctx, query, jobID, workerID)
	if err != nil {
		return err
	}

	// if 0 rows were affected, it means the worker lost lease of this job
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// moves a task from 'running' state to 'available' state
func (db *DBStore) Retry(ctx context.Context, jobID int64, workerID string, attemptErr AttemptError, nextRunAt time.Time) error {
	const query = `
		UPDATE jobs SET state = 'available', run_at = $3, errors = errors || $4::jsonb, locked_by = null, locked_until = null
		WHERE id = $1 AND locked_by = $2 AND state = 'running'
	`
	attemptErrVal, err := json.Marshal([]AttemptError{attemptErr})
	if err != nil {
		return err
	}

	tag, err := db.pool.Exec(ctx, query, jobID, workerID, nextRunAt, attemptErrVal)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// moves a task from 'running' state to 'dead' state
func (db *DBStore) DeadLetter(ctx context.Context, jobID int64, workerID string, attemptErr AttemptError) error {
	const query = `
		UPDATE jobs SET state = 'dead', finished_at = NOW(), errors = errors || $3::jsonb, locked_by = null, locked_until = null
		WHERE id = $1 AND locked_by = $2 AND state = 'running'
	`
	attemptErrVal, err := json.Marshal([]AttemptError{attemptErr})
	if err != nil {
		return err
	}

	tag, err := db.pool.Exec(ctx, query, jobID, workerID, attemptErrVal)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}
