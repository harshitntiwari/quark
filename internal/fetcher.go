package internal

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type FetcherConfig struct {
	Queue         string
	WorkerID      string
	Concurrency   int
	PollInterval  time.Duration
	LeaseDuration time.Duration
}

type Fetcher struct {
	cfg      FetcherConfig
	pool     *pgxpool.Pool
	store    Store
	executor *Executor
	sem      chan struct{}
	wakeCh   chan struct{}
}

// allocates and returns a new [Fetcher]
func NewFetcher(pool *pgxpool.Pool, store Store, executor *Executor, cfg FetcherConfig) *Fetcher {
	return &Fetcher{
		cfg:      cfg,
		pool:     pool,
		store:    store,
		executor: executor,
		sem:      make(chan struct{}, cfg.Concurrency),
		wakeCh:   make(chan struct{}, 1),
	}
}

// polls for available jobs until ctx is cancelled
func (f *Fetcher) Run(ctx context.Context) {
	// will send a tick to a channel ticker.C at every PollInterval
	ticker := time.NewTicker(f.cfg.PollInterval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		f.listen(ctx)
	}()
	defer wg.Wait()

	// do an immediate poll on startup before waiting for the first tick
	f.fetchAndDispatch(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// poll on every tick
			f.fetchAndDispatch(ctx)
		case <-f.wakeCh:
			// poll when a worker is woken up
			f.fetchAndDispatch(ctx)
		}
	}
}

func (f *Fetcher) fetchAndDispatch(ctx context.Context) {
	acquiredSlots := f.acquireSlots()

	if acquiredSlots == 0 {
		return
	}

	jobs, err := f.store.Claim(ctx, ClaimParams{
		Queue:         f.cfg.Queue,
		Limit:         acquiredSlots,
		WorkerID:      f.cfg.WorkerID,
		LeaseDuration: f.cfg.LeaseDuration,
	})

	if err != nil {
		// release all acquired slots
		f.releaseSlots(acquiredSlots)
		return
	}

	if acquiredSlots-len(jobs) > 0 {
		// release the unused slots
		f.releaseSlots(acquiredSlots - len(jobs))
	}

	for _, job := range jobs {
		f.executor.Submit(ctx, job, func() { <-f.sem })
	}

}

func (f *Fetcher) releaseSlots(release int) {
	if release > cap(f.sem) {
		return
	}

	for release > 0 {
		<-f.sem
		release--
	}
}

func (f *Fetcher) acquireSlots() int {
	acquired := 0

	for acquired < f.cfg.Concurrency {
		select {
		case f.sem <- struct{}{}:
			acquired++
		default:
			// channel buffer is full which means all remaining slots are busy
			return acquired
		}
	}
	return acquired
}

func (f *Fetcher) listen(ctx context.Context) {
	for {
		// worker shutting down
		if ctx.Err() != nil {
			return
		}

		err := f.listenOnce(ctx)

		// network drop or db error
		if err != nil && ctx.Err() == nil {
			select {
			// if the worker is shutdown during the 1 second wait,
			// exit immediately instead of waiting for entire 1 second
			case <-ctx.Done():
				return
			// wait for 1 second before looping again to call listenOnce()
			case <-time.After(time.Second):
			}
		}

	}
}

func (f *Fetcher) listenOnce(ctx context.Context) error {
	// acquire a connection from the pool
	conn, err := f.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release() // defer the release of this connection

	// use this connection to subscribe to the channel
	_, err = conn.Exec(ctx, "LISTEN quark_available")
	if err != nil {
		return err
	}

	for {
		// wait for a notification to get published to the channel
		// blocks until a notification is received
		notification, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		// if the queue in notification matches this worker's queue
		if notification.Payload == f.cfg.Queue {
			select {
			case f.wakeCh <- struct{}{}:
			default:
			}
		}
	}
}
