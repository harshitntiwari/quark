package internal

import (
	"context"
	"time"
)

type ReaperConfig struct {

	// how often the reaper wakes up to 
	// reap running jobs whose lease has expired 
	// and delete completed jobs
	Interval time.Duration

	// how long to keep completed jobs before deleting them
	RetentionPeriod time.Duration
}

type Reaper struct {
	cfg   ReaperConfig
	store Store
}

// allocates and returns a new [Reaper]
func NewReaper(store Store, cfg ReaperConfig) *Reaper {
	return &Reaper{
		cfg: cfg,
		store: store,
	}
}

func (r *Reaper)Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// reap on every tick
			r.reap(ctx)
		}
	}
}

func (r *Reaper)reap(ctx context.Context) {
	_, _ = r.store.ReapExpiredLeases(ctx)

	_, _ = r.store.DeleteCompletedJobs(ctx, time.Now().Add(-r.cfg.RetentionPeriod))
}