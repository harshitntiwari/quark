package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

const schedulerLockKey int64 = 7829104432

type SchedulerConfig struct {
	Interval time.Duration
	BatchSize int
}

type Scheduler struct {
	pool  *pgxpool.Pool
	store Store
	cfg   SchedulerConfig
}

// allocates and returns a new [Scheduler]
func NewScheduler(pool *pgxpool.Pool, store Store, cfg SchedulerConfig) *Scheduler {
	return &Scheduler{
		pool:  pool,
		store: store,
		cfg:   cfg,
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	s.tick(ctx)

	for {
		select {
		case <- ctx.Done():
			return
		case <- ticker.C:
			s.tick(ctx)
		}
	}
	
}

func (s *Scheduler) tick(ctx context.Context) {
	// acquire a connection from the pool
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer conn.Release() // defer the release of this connection

	var acquired bool
	// acquire advisory lock
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", schedulerLockKey).Scan(&acquired); err != nil || !acquired {
    	// error acquiring lock or another worker is currently the active scheduler leader
		return
	}
	// defer release of advisory lock
	defer func(){
		// ctx could've already been cancelled when this is running, so pass context.Background() instead of ctx
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", schedulerLockKey)
	}()

	now := time.Now()
	// fetch all due schedules from db
	schedules, err := s.store.ListDueSchedules(ctx, now, s.cfg.BatchSize)
	if err != nil {
		return
	}

	// process each due schedule
	for _, sched := range schedules {
		s.processSchedule(ctx, sched, now)
	}
}

func (s *Scheduler) processSchedule(ctx context.Context, sched Schedule, now time.Time) {
	cronSched, err := cron.ParseStandard(sched.CronExpr)
	if err != nil {
		return
	}

	// collect all due slots for this schedule
	var dueSlots []time.Time // will contain all the due timestamps <= now
	slot := sched.NextRunAt

	// while slot <= now
	for !slot.After(now) {
		dueSlots = append(dueSlots, slot) // append slot
		slot = cronSched.Next(slot) // increment slot
	}
	// slot is now the first future timestamp strictly > now (i.e. new nextRunAt)

	if len(dueSlots) == 0 {
		return
	}

	// Record the latest due slot before misfire filtering
	lastDueSlot := dueSlots[len(dueSlots)-1]

	// if no. of missed slots > 1, apply misfire policy
	if len(dueSlots) > 1 {
		switch sched.MisfirePolicy {
    	case MisfireSkip:
			dueSlots = nil // skip enqueuing any missed jobs
		case MisfireCatchUp: // dueSlots remain as is, enqueue all missed jobs
		case MisfireRunOnce:
			fallthrough
		default:
			dueSlots = dueSlots[len(dueSlots)-1:] // most recent missed job
		}
	}

	jobs := make([]InsertParams, 0, len(dueSlots))
	for _, t := range dueSlots {
		key := fmt.Sprintf("schedule:%s:%s", sched.Name, t.UTC().Format(time.RFC3339))
		jobs = append(jobs, InsertParams{
			Queue: sched.Queue,
			Kind: sched.Kind,
			Args: sched.Args,
			UniqueKey: &key,
			Priority: sched.Priority,
			MaxAttempts: sched.MaxAttempts,
			RunAt: t,
			Timeout: sched.Timeout,
		})
	}
	s.store.AdvanceScheduleAndEnqueue(ctx, sched.Name, lastDueSlot, slot, jobs)
}