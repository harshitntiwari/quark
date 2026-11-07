package quark_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/harshitntiwari/quark"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EmailArgs is a sample JobArgs implementation used across end-to-end tests.
type EmailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailArgs) Kind() string {
	return "send_email"
}

// setupTestDB connects to Postgres, runs migrations, and truncates tables for a clean test state.
func setupTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:1234@localhost:5432/quark_test?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping integration test: could not connect to postgres: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping integration test: postgres ping failed (%s): %v", dsn, err)
	}

	if err := quark.Migrate(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("Migrate failed: %v", err)
	}

	if _, err := pool.Exec(ctx, "TRUNCATE TABLE jobs, schedules RESTART IDENTITY"); err != nil {
		pool.Close()
		t.Fatalf("failed to truncate tables: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

// getJob fetches the latest state of a job row directly from Postgres for test assertions.
func getJob(t *testing.T, pool *pgxpool.Pool, id int64) (quark.JobState, int, []byte) {
	t.Helper()
	var state quark.JobState
	var attempts int
	var errsJSON []byte
	err := pool.QueryRow(context.Background(),
		"SELECT state, attempts, errors FROM jobs WHERE id = $1", id,
	).Scan(&state, &attempts, &errsJSON)
	if err != nil {
		t.Fatalf("failed to query job %d: %v", id, err)
	}
	return state, attempts, errsJSON
}

// 1. Tests Enqueue + LISTEN/NOTIFY instant wake-up + Type-safe Handler execution
func TestEndToEnd_HappyPathAndListenNotify(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()

	client, err := quark.NewClient(pool, nil)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// Set PollInterval very high (10s) to prove LISTEN/NOTIFY wakes the worker immediately
	worker := quark.NewWorker(pool, &quark.WorkerConfig{
		Queue:        "default",
		Concurrency:  5,
		PollInterval: 10 * time.Second,
	})

	doneCh := make(chan EmailArgs, 1)
	quark.RegisterHandler(worker, func(ctx context.Context, job quark.Job, args EmailArgs) error {
		doneCh <- args
		return nil
	})

	worker.Start(ctx)
	defer worker.Stop()

	// Give listener goroutine a brief moment to establish LISTEN connection
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	enqueued, err := client.Enqueue(ctx, EmailArgs{
		To:      "harshit@example.com",
		Subject: "Hello Quark",
	}, nil)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	select {
	case got := <-doneCh:
		elapsed := time.Since(start)
		if got.To != "harshit@example.com" || got.Subject != "Hello Quark" {
			t.Fatalf("unexpected args received: %+v", got)
		}
		if elapsed > 2*time.Second {
			t.Errorf("expected instant wake-up via LISTEN/NOTIFY, but took %v", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for job to be processed")
	}

	// Stop worker to ensure Complete() DB update has finished before querying state
	worker.Stop()

	state, attempts, _ := getJob(t, pool, enqueued.ID)
	if state != quark.Completed {
		t.Errorf("expected state %q, got %q", quark.Completed, state)
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
}

// 2. Tests Transactional Enqueue (Rollback vs Commit) and UniqueKey Deduplication
func TestEndToEnd_TransactionalEnqueueAndUniqueKey(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()

	client, err := quark.NewClient(pool, nil)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	// Part A: EnqueueTx followed by Rollback should not persist the job
	tx1, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("pool.Begin failed: %v", err)
	}
	_, err = client.EnqueueTx(ctx, tx1, EmailArgs{To: "rollback@example.com"}, nil)
	if err != nil {
		t.Fatalf("EnqueueTx failed: %v", err)
	}
	_ = tx1.Rollback(ctx)

	var count int
	_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs").Scan(&count)
	if count != 0 {
		t.Fatalf("expected 0 jobs after transaction rollback, got %d", count)
	}

	// Part B: UniqueKey deduplication
	opts := &quark.EnqueueOpts{UniqueKey: "welcome-email-user-99"}
	_, err = client.Enqueue(ctx, EmailArgs{To: "user99@example.com"}, opts)
	if err != nil {
		t.Fatalf("first Enqueue with UniqueKey failed: %v", err)
	}

	_, err = client.Enqueue(ctx, EmailArgs{To: "user99@example.com"}, opts)
	if !errors.Is(err, quark.ErrDuplicateJob) {
		t.Fatalf("expected ErrDuplicateJob on duplicate UniqueKey, got: %v", err)
	}
}

// 3. Tests EnqueueBatch and Worker Concurrency Semaphore Enforcement
func TestEndToEnd_BatchAndConcurrencyLimit(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()

	client, _ := quark.NewClient(pool, nil)

	const totalJobs = 20
	const maxConcurrency = 4

	items := make([]quark.BatchItem, totalJobs)
	for i := range totalJobs {
		items[i] = quark.BatchItem{
			Args: EmailArgs{To: fmt.Sprintf("user%d@example.com", i)},
		}
	}

	jobs, err := client.EnqueueBatch(ctx, items)
	if err != nil {
		t.Fatalf("EnqueueBatch failed: %v", err)
	}
	if len(jobs) != totalJobs {
		t.Fatalf("expected %d jobs, got %d", totalJobs, len(jobs))
	}

	var currentActive atomic.Int32
	var peakActive atomic.Int32
	var completedCount atomic.Int32
	allDone := make(chan struct{})

	worker := quark.NewWorker(pool, &quark.WorkerConfig{
		Queue:        "default",
		Concurrency:  maxConcurrency,
		PollInterval: 50 * time.Millisecond,
	})

	quark.RegisterHandler(worker, func(ctx context.Context, job quark.Job, args EmailArgs) error {
		cur := currentActive.Add(1)
		for {
			prevPeak := peakActive.Load()
			if cur <= prevPeak || peakActive.CompareAndSwap(prevPeak, cur) {
				break
			}
		}

		time.Sleep(40 * time.Millisecond) // hold slot briefly to force overlap
		currentActive.Add(-1)

		if completedCount.Add(1) == totalJobs {
			close(allDone)
		}
		return nil
	})

	worker.Start(ctx)
	defer worker.Stop()

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for batch jobs, completed %d/%d", completedCount.Load(), totalJobs)
	}

	if peak := peakActive.Load(); peak > maxConcurrency {
		t.Errorf("concurrency limit violated: peak active was %d, limit was %d", peak, maxConcurrency)
	}
}

// 4. Tests Retry with Backoff, NonRetryable, Snooze, and Panic Recovery
func TestEndToEnd_RetriesSnoozeNonRetryableAndPanic(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()

	client, _ := quark.NewClient(pool, nil)

	worker := quark.NewWorker(pool, &quark.WorkerConfig{
		Queue:          "default",
		Concurrency:    5,
		PollInterval:   50 * time.Millisecond,
		RetryBaseDelay: 50 * time.Millisecond,
		RetryMaxDelay:  100 * time.Millisecond,
	})

	// Handler 1: Fails twice, succeeds on 3rd attempt
	retryDone := make(chan struct{})
	worker.Register("flaky_job", func(ctx context.Context, job quark.Job) error {
		if job.Attempts < 3 {
			return fmt.Errorf("temporary network failure on attempt %d", job.Attempts)
		}
		close(retryDone)
		return nil
	})

	// Handler 2: Returns NonRetryable on 1st attempt
	nonRetryDone := make(chan struct{})
	worker.Register("fatal_job", func(ctx context.Context, job quark.Job) error {
		defer close(nonRetryDone)
		return quark.NonRetryable(errors.New("invalid payload"))
	})

	// Handler 3: Snoozes on 1st execution, succeeds on 2nd
	var snoozeRuns atomic.Int32
	var finalSnoozeAttempts atomic.Int32
	snoozeDone := make(chan struct{})
	worker.Register("snooze_job", func(ctx context.Context, job quark.Job) error {
		if snoozeRuns.Add(1) == 1 {
			return quark.Snooze(80 * time.Millisecond)
		}
		finalSnoozeAttempts.Store(int32(job.Attempts))
		close(snoozeDone)
		return nil
	})

	// Handler 4: Panics inside handler
	panicDone := make(chan struct{})
	worker.Register("panic_job", func(ctx context.Context, job quark.Job) error {
		defer close(panicDone)
		panic("unexpected nil pointer")
	})

	worker.Start(ctx)
	defer worker.Stop()

	jRetry, _ := client.Enqueue(ctx, rawKindArgs("flaky_job"), &quark.EnqueueOpts{MaxAttempts: 3})
	jFatal, _ := client.Enqueue(ctx, rawKindArgs("fatal_job"), &quark.EnqueueOpts{MaxAttempts: 5})
	jSnooze, _ := client.Enqueue(ctx, rawKindArgs("snooze_job"), &quark.EnqueueOpts{MaxAttempts: 3})
	jPanic, _ := client.Enqueue(ctx, rawKindArgs("panic_job"), &quark.EnqueueOpts{MaxAttempts: 1})

	for _, ch := range []chan struct{}{retryDone, nonRetryDone, snoozeDone, panicDone} {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for error-handling test jobs")
		}
	}

	worker.Stop()

	// Assert Retry job completed on attempt 3
	if state, attempts, _ := getJob(t, pool, jRetry.ID); state != quark.Completed || attempts != 3 {
		t.Errorf("flaky_job: expected (completed, 3), got (%s, %d)", state, attempts)
	}

	// Assert NonRetryable job went straight to dead on attempt 1 (despite MaxAttempts: 5)
	if state, attempts, _ := getJob(t, pool, jFatal.ID); state != quark.Dead || attempts != 1 {
		t.Errorf("fatal_job: expected (dead, 1), got (%s, %d)", state, attempts)
	}

	// Assert Snooze job did not consume an extra attempt when snoozed
	if state, attempts, _ := getJob(t, pool, jSnooze.ID); state != quark.Completed || attempts != 1 {
		t.Errorf("snooze_job: expected (completed, 1), got (%s, %d)", state, attempts)
	}

	// Assert Panic job was recovered and moved to dead
	if state, attempts, _ := getJob(t, pool, jPanic.ID); state != quark.Dead || attempts != 1 {
		t.Errorf("panic_job: expected (dead, 1), got (%s, %d)", state, attempts)
	}
}

// 5. Tests Lease Heartbeat Renewal & Reaper Expired-Lease Recovery
func TestEndToEnd_HeartbeatAndReaperRecovery(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()

	client, _ := quark.NewClient(pool, nil)

	// Simulate a zombie job left behind by a crashed worker (state='running', locked_until in the past)
	zombieJob, err := client.Enqueue(ctx, EmailArgs{To: "zombie@example.com"}, nil)
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	_, err = pool.Exec(ctx, `
		UPDATE jobs
		SET state = 'running', attempts = 1, locked_by = 'dead-worker-1', locked_until = NOW() - INTERVAL '10 seconds'
		WHERE id = $1
	`, zombieJob.ID)
	if err != nil {
		t.Fatalf("failed to set up zombie job: %v", err)
	}

	doneCh := make(chan struct{})
	worker := quark.NewWorker(pool, &quark.WorkerConfig{
		Queue:         "default",
		PollInterval:  50 * time.Millisecond,
		LeaseDuration: 300 * time.Millisecond, // short lease (heartbeats every 100ms)
		ReapInterval:  100 * time.Millisecond, // reaper runs every 100ms
	})

	quark.RegisterHandler(worker, func(ctx context.Context, job quark.Job, args EmailArgs) error {
		// Sleep longer than LeaseDuration (500ms > 300ms) to prove Heartbeat keeps lease alive
		time.Sleep(500 * time.Millisecond)
		close(doneCh)
		return nil
	})

	worker.Start(ctx)
	defer worker.Stop()

	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reaper to recover zombie job and worker to complete it")
	}

	worker.Stop()

	state, attempts, _ := getJob(t, pool, zombieJob.ID)
	if state != quark.Completed || attempts != 2 {
		t.Errorf("zombie job: expected (completed, 2), got (%s, %d)", state, attempts)
	}
}

// 6. Tests Recurring Cron Scheduler
func TestEndToEnd_CronScheduler(t *testing.T) {
	pool := setupTestDB(t)
	ctx := context.Background()

	client, _ := quark.NewClient(pool, nil)

	sched, err := client.AddSchedule(ctx, "every_minute_sync", "* * * * *", EmailArgs{
		To:      "cron@example.com",
		Subject: "Scheduled Report",
	}, nil)
	if err != nil {
		t.Fatalf("AddSchedule failed: %v", err)
	}

	// Move next_run_at to 1 second in the past so the scheduler fires immediately on startup
	_, err = pool.Exec(ctx, "UPDATE schedules SET next_run_at = NOW() - INTERVAL '1 second' WHERE name = $1", sched.Name)
	if err != nil {
		t.Fatalf("failed to backdate schedule next_run_at: %v", err)
	}

	cronDone := make(chan EmailArgs, 1)
	worker := quark.NewWorker(pool, &quark.WorkerConfig{
		Queue:            "default",
		PollInterval:     50 * time.Millisecond,
		ScheduleInterval: 100 * time.Millisecond,
	})

	quark.RegisterHandler(worker, func(ctx context.Context, job quark.Job, args EmailArgs) error {
		cronDone <- args
		return nil
	})

	worker.Start(ctx)
	defer worker.Stop()

	select {
	case got := <-cronDone:
		if got.To != "cron@example.com" {
			t.Errorf("unexpected cron job args: %+v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for scheduled cron job to execute")
	}
}

type rawKindArgs string

func (k rawKindArgs) Kind() string { return string(k) }