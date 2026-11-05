# <p align="center">Quark</p>

<p align="center"> A lightweight, reliable and distributed PostgreSQL-backed background job queue for Go built on `pgx/v5`.</p>

![Alt text](docs/images/arch.svg?v=2)

## Features

- **Transactional Enqueuing:** Enqueue jobs inside your existing database transactions (`EnqueueTx`, `EnqueueBatchTx`) so jobs only exist if your business transaction commits.
- **Low-Latency & High Throughput:** Instant worker wake-ups via PostgreSQL `LISTEN/NOTIFY` with fallback polling and single-round-trip batch inserts (`EnqueueBatch`).
- **Type-Safe Generics:** Define strongly-typed job payloads and register generic handlers (`RegisterHandler[T]`) without manual JSON unmarshaling boilerplate.
- **Automatic Retries & Custom Error Control:** Exponential backoff with jitter by default, plus `NonRetryable` for immediate dead-lettering and `Snooze` to reschedule without consuming an attempt.
- **Lease Heartbeats & Crash Recovery:** Running jobs automatically extend their leases via background heartbeats. If a lease is lost, the handler's `context.Context` is cancelled immediately. A background reaper recovers zombie jobs from crashed workers and purges old completed jobs.
- **Deduplication & Scheduling:** Built-in idempotency via `UniqueKey`, priority ordering (`0` = highest), and delayed execution (`Delay` / `RunAt`).

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
