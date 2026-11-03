package quark

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/harshitntiwari/quark/internal"
	"github.com/jackc/pgx/v5/pgxpool"
)

// package defaults for worker config
const (
	defaultConcurrency     = 10
	defaultPollInterval    = 500 * time.Millisecond
	defaultLeaseDuration   = 30 * time.Second
	defaultRetryBaseDelay  = 1 * time.Second
	defaultRetryMaxDelay   = 5 * time.Minute
	defaultReapInterval    = 10 * time.Second
	defaultRetentionPeriod = 24 * time.Hour
)

// WorkerConfig configures concurrency, polling, lease, retry, and cleanup
// behavior for a [Worker]. Zero values fall back to package defaults.
type WorkerConfig struct {
	// Queue is the name of the queue this worker claims jobs from.
	// Defaults to "default".
	Queue           string

	// WorkerID uniquely identifies this worker instance as a lease holder.
	// Defaults to a randomly generated ID ("worker-<hex>").
	WorkerID        string

	// Concurrency is the maximum number of jobs this worker may execute in parallel.
	// Defaults to 10.
	Concurrency     int

	// PollInterval is how often the worker polls the database for available jobs
	// as a fallback to LISTEN/NOTIFY wake-ups. Defaults to 500ms.
	PollInterval    time.Duration

	// LeaseDuration is how long a claimed job lock is held before expiring if not
	// renewed by background heartbeats. Defaults to 30s.
	LeaseDuration   time.Duration

	// RetryBaseDelay is the initial backoff duration before the first retry of a
	// failed job. Defaults to 1s.
	RetryBaseDelay  time.Duration

	// RetryMaxDelay caps the exponential backoff duration between retries.
	// Defaults to 5m.
	RetryMaxDelay   time.Duration

	// ReapInterval is how often the background reaper scans for expired leases
	// and purges old completed jobs. Defaults to 10s.
	ReapInterval    time.Duration

	// RetentionPeriod is how long completed jobs are kept in the database before
	// being deleted by the reaper. Defaults to 24h.
	RetentionPeriod time.Duration
}

type Worker struct {
	cfg      WorkerConfig
	executor *internal.Executor
	fetcher  *internal.Fetcher
	reaper   *internal.Reaper

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func generateWorkerID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b) // fill array with random bytes
	return "worker-" + hex.EncodeToString(b)
}

// NewWorker creates and returns a new quark consumer [Worker]
func NewWorker(pool *pgxpool.Pool, cfg *WorkerConfig) *Worker {
	var c WorkerConfig
	if cfg != nil {
		c = *cfg
	}
	c.Queue = cmp.Or(c.Queue, string(defaultQueue))
	if c.WorkerID == "" {
		c.WorkerID = generateWorkerID()
	}
	c.Concurrency = cmp.Or(c.Concurrency, int(defaultConcurrency))
	c.PollInterval = cmp.Or(c.PollInterval, defaultPollInterval)
	c.LeaseDuration = cmp.Or(c.LeaseDuration, defaultLeaseDuration)
	c.RetryBaseDelay = cmp.Or(c.RetryBaseDelay, defaultRetryBaseDelay)
	c.RetryMaxDelay = cmp.Or(c.RetryMaxDelay, defaultRetryMaxDelay)
	c.ReapInterval = cmp.Or(c.ReapInterval, defaultReapInterval)
	c.RetentionPeriod = cmp.Or(c.RetentionPeriod, defaultRetentionPeriod)
	store := internal.NewDBStore(pool)
	executor := internal.NewExecutor(store, internal.ExecutorConfig{
		WorkerID:       c.WorkerID,
		RetryBaseDelay: c.RetryBaseDelay,
		RetryMaxDelay:  c.RetryMaxDelay,
		LeaseDuration:  c.LeaseDuration,
	})
	fetcher := internal.NewFetcher(pool, store, executor, internal.FetcherConfig{
		Queue:         c.Queue,
		WorkerID:      c.WorkerID,
		Concurrency:   c.Concurrency,
		PollInterval:  c.PollInterval,
		LeaseDuration: c.LeaseDuration,
	})
	reaper := internal.NewReaper(store, internal.ReaperConfig{
		Interval:        c.ReapInterval,
		RetentionPeriod: c.RetentionPeriod,
	})
	return &Worker{
		cfg:      c,
		executor: executor,
		fetcher:  fetcher,
		reaper:   reaper,
	}
}

// RegisterHandler registers a type-safe handler function that automatically unmarshals job arguments into T.
func RegisterHandler[T JobArgs](w *Worker, fn func(ctx context.Context, job Job, args T) error) {
	var zero T // zero value of struct type T
	kind := zero.Kind()

	w.Register(kind, func(ctx context.Context, job Job) error {
		var args T
		if err := json.Unmarshal(job.Args, &args); err != nil {
			return NonRetryable(err)
		}
		return fn(ctx, job, args)
	})
}

// Register binds a raw handler function to the specified job kind string.
func (w *Worker) Register(kind string, fn func(ctx context.Context, job Job) error) {
	w.executor.Register(kind, fn)
}

// Start launches the background job fetcher, listener, and lease reaper goroutines.
// Begins polling jobs from queue and executing them in background
func (w *Worker) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel

	w.wg.Add(2)
	go func() {
		defer w.wg.Done()
		w.reaper.Run(runCtx)
	}()

	
	go func() {
		defer w.wg.Done()
		w.fetcher.Run(runCtx)
	}()
}

// Stop halts new job claims and blocks until all in-flight jobs and background loops have finished.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()       // wait for fetcher.Run() to return
	w.executor.Wait() // wait for in-flight jobs to finish
}
