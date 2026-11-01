package internal

import (
	"context"
	"time"
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
	store    Store
	executor *Executor
	sem      chan struct{}
}

// allocates and returns a new [Fetcher]
func NewFetcher(store Store, executor *Executor, cfg FetcherConfig) *Fetcher {
	return &Fetcher{
		cfg:      cfg,
		store:    store,
		executor: executor,
		sem:      make(chan struct{}, cfg.Concurrency),
	}
}

// polls for available jobs until ctx is cancelled
func (f *Fetcher) Run(ctx context.Context) {
	// will send a tick to a channel ticker.C at every PollInterval
	ticker := time.NewTicker(f.cfg.PollInterval)
	defer ticker.Stop()

	// do an immediate poll on startup before waiting for the first tick
	f.fetchAndDispatch(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// poll on every tick
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
		Queue: f.cfg.Queue, 
		Limit: acquiredSlots, 
		WorkerID: f.cfg.WorkerID, 
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
		<- f.sem
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
