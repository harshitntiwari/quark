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

const (
	defaultConcurrency     = 10
	defaultPollInterval    = 500 * time.Millisecond
	defaultLeaseDuration   = 30 * time.Second
	defaultRetryBaseDelay  = 1 * time.Second
	defaultRetryMaxDelay   = 5 * time.Minute
	defaultReapInterval    = 10 * time.Second
	defaultRetentionPeriod = 24 * time.Hour
)

type WorkerConfig struct {
	Queue           string
	WorkerID        string
	Concurrency     int
	PollInterval    time.Duration
	LeaseDuration   time.Duration
	RetryBaseDelay  time.Duration
	RetryMaxDelay   time.Duration
	ReapInterval    time.Duration
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

// creates and returns a new quark consumer [Worker]
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
	fetcher := internal.NewFetcher(store, executor, internal.FetcherConfig{
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

func (w *Worker) Register(kind string, fn func(ctx context.Context, job Job) error) {
	w.executor.Register(kind, fn)
}

// begins polling jobs from queue and executing them in background
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

// stops claiming new jobs and waits for all in-flight jobs to complete
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()       // wait for fetcher.Run() to return
	w.executor.Wait() // wait for in-flight jobs to finish
}
