# <p align="center">Quark</p>

<p align="center">
  <a href="https://github.com/harshitntiwari/quark/actions/workflows/ci.yml"><img src="https://github.com/harshitntiwari/quark/actions/workflows/ci.yml/badge.svg" alt="CI Status"></a>
  <a href="https://pkg.go.dev/github.com/harshitntiwari/quark"><img src="https://pkg.go.dev/badge/github.com/harshitntiwari/quark.svg" alt="Go Reference"></a>
</p>

<p align="center"> A lightweight, reliable and distributed PostgreSQL-backed background job queue for Go built on `pgx/v5`.</p>

![Alt text](docs/images/arch.svg?v=2)

## Features

- **Transactional Enqueuing:** Enqueue jobs inside your existing database transactions (`EnqueueTx`, `EnqueueBatchTx`) so jobs only exist if your business transaction commits.
- **Low-Latency & High Throughput:** Instant worker wake-ups via PostgreSQL `LISTEN/NOTIFY` with fallback polling and single-round-trip batch inserts (`EnqueueBatch`).
- **Type-Safe Generics:** Define strongly-typed job payloads and register generic handlers (`RegisterHandler[T]`) without manual JSON unmarshaling boilerplate.
- **Automatic Retries & Custom Error Control:** Exponential backoff with jitter by default, plus `NonRetryable` for immediate dead-lettering and `Snooze` to reschedule without consuming an attempt.
- **Lease Heartbeats & Crash Recovery:** Running jobs automatically extend their leases via background heartbeats. If a lease is lost, the handler's `context.Context` is cancelled immediately. A background reaper recovers zombie jobs from crashed workers and purges old completed jobs.
- **Deduplication & Delayed Execution:** Built-in idempotency via `UniqueKey`, priority ordering (`0` = highest), and delayed execution (`Delay` / `RunAt`).
- **Distributed Cron Scheduling:** Register recurring cron jobs (`AddSchedule`, `AddScheduleTx`) backed by PostgreSQL advisory-lock leader election, deterministic slot deduplication, and configurable misfire policies (`MisfireSkip`, `MisfireRunOnce`, `MisfireCatchUp`).

## Installation

```bash
go get github.com/harshitntiwari/quark
```

## Quick start

1. Run Database Migrations

   Before enqueuing or processing jobs, initialize the Quark database schema using your `pgxpool.Pool`:

   ```go
   pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
   if err != nil {
       log.Fatal(err)
   }
   defer pool.Close()

   if err := quark.Migrate(ctx, pool); err != nil {
       log.Fatal(err)
   }
   ```

2. Define a Job Payload

   Implement the `quark.JobArgs` interface by adding a `Kind() string` method to your payload struct:

   ```go
    type SendEmailArgs struct {
        To      string `json:"to"`
        Subject string `json:"subject"`
    }

    func (SendEmailArgs) Kind() string {
        return "send_email"
    }
   ```

### Producer Guide (Enqueuing Jobs)

#### Initializing the Client

Pass `nil` to use default settings (`DefaultQueue: "default"`, `DefaultMaxAttempts: 3`, `DefaultTimeout: 60s`), or provide a custom `*quark.ClientConfig`:

```go
client, err := quark.NewClient(pool, &quark.ClientConfig{
    DefaultQueue:       "default",
    DefaultMaxAttempts: 5,
    DefaultTimeout:     30 * time.Second,
})
if err != nil {
    log.Fatal(err)
}
```

#### Enqueuing a Single Job (`Enqueue`)

```go
// Enqueue with default options
job, err := client.Enqueue(ctx, SendEmailArgs{
    To:      "user@example.com",
    Subject: "Welcome!",
}, nil)

// Enqueue with per-job option overrides
job, err = client.Enqueue(ctx, SendEmailArgs{
    To:      "user@example.com",
    Subject: "Trial Expiring Soon",
}, &quark.EnqueueOpts{
    Queue:       "email_queue",
    Priority:    0,                  // 0 is highest priority
    Delay:       15 * time.Minute,   // or use RunAt: time.Now().Add(...)
    MaxAttempts: 3,
    Timeout:     10 * time.Second,
    UniqueKey:   "trial-exp-user-42", // returns quark.ErrDuplicateJob if already active
})
```

#### Transactional Enqueue (`EnqueueTx`)

Guarantee that a job is only enqueued if your surrounding database changes commit:

```go
tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(ctx)

// 1. Perform your application SQL queries on tx
_, err = tx.Exec(ctx, "INSERT INTO users (email) VALUES ($1)", "user@example.com")
if err != nil {
    return err
}

// 2. Enqueue the job inside the exact same transaction
_, err = client.EnqueueTx(ctx, tx, SendEmailArgs{
    To:      "user@example.com",
    Subject: "Welcome!",
}, nil)
if err != nil {
    return err
}

return tx.Commit(ctx)
```

#### Batch Enqueue (`EnqueueBatch` & `EnqueueBatchTx`)

Insert hundreds or thousands of jobs in a single network round-trip

```go
items := []quark.BatchItem{
    {
        Args: SendEmailArgs{To: "alice@example.com", Subject: "Invoice #1"},
        Opts: &quark.EnqueueOpts{Priority: 1},
    },
    {
        Args: SendEmailArgs{To: "bob@example.com", Subject: "Invoice #2"},
        Opts: &quark.EnqueueOpts{Priority: 1},
    },
}

jobs, err := client.EnqueueBatch(ctx, items)
```

#### Recurring Cron Schedules (`AddSchedule` & `AddScheduleTx`)

Register or update recurring cron schedules idempotently using standard 5-field cron expressions (`minute hour day-of-month month day-of-week`). Across a multi-node cluster, Quark uses PostgreSQL advisory locks (`pg_try_advisory_lock`) to elect a single active scheduler leader and attaches a deterministic `UniqueKey` (`schedule:<name>:<slot_time>`) to every generated job to guarantee at-most-once slot enqueuing.

```go
// Register a schedule with default options
sched, err := client.AddSchedule(
    ctx,
    "daily_digest_email", // unique schedule name (upserts on conflict)
    "0 9 * * *",          // every day at 09:00 UTC
    SendEmailArgs{
        To:      "team@example.com",
        Subject: "Daily Digest",
    },
    nil,
)

// Register a schedule with custom overrides and misfire policy
sched, err = client.AddSchedule(
    ctx,
    "hourly_invoice_sync",
    "0 * * * *", // top of every hour
    SendEmailArgs{
        To:      "billing@example.com",
        Subject: "Hourly Sync Report",
    },
    &quark.ScheduleOpts{
        Queue:         "email_queue",
        Priority:      1,
        MaxAttempts:   5,
        Timeout:       2 * time.Minute,
        MisfirePolicy: quark.MisfireRunOnce,
        Disabled:      false,
    },
)
```

You can also register or update a schedule atomically within an existing database transaction using `AddScheduleTx`:

```go
sched, err := client.AddScheduleTx(ctx, tx, "daily_digest_email", "0 9 * * *", args, nil)
```

##### Misfire Policies

If all workers are offline during one or more scheduled firing windows, `MisfirePolicy` controls how the scheduler recovers when it comes back online:

- **`quark.MisfireRunOnce`**: Enqueues a single job for the most recent missed slot and advances `next_run_at` to the next future slot.
- **`quark.MisfireSkip`**: Skips all missed slots without enqueuing any jobs and advances `next_run_at` to the next future slot.
- **`quark.MisfireCatchUp`**: Enqueues a separate job for every missed slot in chronological order and advances `next_run_at` to the next future slot.

### Consumer Guide (Running Workers)

##### Initializing and Starting a Worker

Create a Worker, register your job handlers, and call Start(ctx)

```go
worker := quark.NewWorker(pool, &quark.WorkerConfig{
    Queue:           "default",
    Concurrency:     20,
    PollInterval:    500 * time.Millisecond,
    LeaseDuration:   30 * time.Second,
    RetryBaseDelay:  1 * time.Second,
    RetryMaxDelay:   5 * time.Minute,
    ReapInterval:    10 * time.Second,
    RetentionPeriod: 24 * time.Hour,
    ScheduleInterval:  1 * time.Second,
    ScheduleBatchSize: 100,
})

// Register a strongly-typed handler using generics
quark.RegisterHandler(worker, func(ctx context.Context, job quark.Job, args SendEmailArgs) error {
    fmt.Printf("Processing job %d: sending email to %s\n", job.ID, args.To)
    return nil
})

// Start worker
worker.Start(ctx)

// Wait for termination signal (SIGINT / SIGTERM) and shut down gracefully
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
<-sigCh

// Stop claiming new jobs and wait for in-flight jobs to complete
worker.Stop()
```

#### Controlling Retries, Dead-Lettering, and Snoozing

Inside any handler function, the returned error controls the job's state transition:

- **Success** (`return nil`): Marks the job as completed.
- **Standard Retry** (`return err`): Logs the attempt error and reschedules the job using exponential backoff + jitter. Once job.Attempts >= job.MaxAttempts, the job moves to the dead state automatically.
- **Immediate Dead-Letter** (`quark.NonRetryable`): Skips any remaining retry attempts and transitions the job directly to dead:

  ```go
  if errors.Is(err, ErrInvalidEmailAddress) {
      return quark.NonRetryable(err)
  }
  ```

- **Snooze** (`quark.Snooze`): Reschedules the job to run after a given duration without counting as a failed attempt or recording an error (useful for downstream rate limits):

  ```go
  if rateLimited {
      return quark.Snooze(30 * time.Second)
  }
  ```

## Development & Testing

### Prerequisites

- **Go:** 1.22 or higher
- **PostgreSQL:** 14 or higher (or Docker to run a local test container)

### 1. Clone the Repository

```bash
git clone https://github.com/harshitntiwari/quark.git
cd quark
go mod download
```

### 2. Start a Local PostgreSQL Test Instance

Spin up an isolated PostgreSQL container via Docker:

```bash
docker run --name quark-postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=quark_test -p 5432:5432 -d postgres:16-alpine
```

### 3. Run the End-to-End Integration Test Suite

The integration test suite in `quark_test.go` verifies transactional enqueuing, `LISTEN/NOTIFY` wake-ups, batching, concurrency semaphores, retry backoff, `Snooze`, `NonRetryable`, panic recovery, lease heartbeats, expired-lease reaping, and cron scheduling against a live PostgreSQL instance.

By default, tests connect to `postgres://postgres:1234@localhost:5432/quark_test?sslmode=disable`. You can override this by setting `DATABASE_URL`:

```bash
export DATABASE_URL="postgres://<username>:<password>@localhost:5432/<db_name>?sslmode=disable"

# Run all tests with the Go race detector enabled
go test -v -race ./...
```

### 4. Project Structure

```text
quark/
├── client.go          # Producer API (Enqueue, EnqueueTx, EnqueueBatch, AddSchedule)
├── worker.go          # Consumer lifecycle, configuration, and generic handler registration
├── job.go             # Public domain types, job states, and error wrappers
├── migrate.go         # Embedded SQL migration runner with advisory locking
├── quark_test.go      # End-to-end integration test suite
├── migrations/        # SQL schema definitions (jobs, schedules, indexes)
└── internal/
    ├── db.go          # PostgreSQL queries (SKIP LOCKED claiming, heartbeats, batching)
    ├── store.go       # Storage interface and parameter structs
    ├── fetcher.go     # Concurrency semaphore, polling loop, and LISTEN/NOTIFY subscriber
    ├── executor.go    # Job execution pipeline, panic recovery, timeouts, and heartbeating
    ├── reaper.go      # Expired lease recovery and completed job retention cleanup
    ├── scheduler.go   # Leader-elected cron evaluation and misfire handling
    ├── retry.go       # Exponential backoff with equal jitter calculation
    ├── model.go       # Core domain structs (Job, Schedule, AttemptError)
    ├── state.go       # JobState constants
    └── errors.go      # Sentinel and custom error types
```
