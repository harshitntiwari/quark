package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type ExecutorConfig struct {
	WorkerID       string
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	LeaseDuration  time.Duration
}

type HandlerFunc func(ctx context.Context, job Job) error

type Executor struct {
	cfg      ExecutorConfig
	store    Store
	handlers map[string]HandlerFunc
	wg       sync.WaitGroup
}

// allocates and returns a new [Executor]
func NewExecutor(store Store, cfg ExecutorConfig) *Executor {
	return &Executor{
		cfg:      cfg,
		store:    store,
		handlers: map[string]HandlerFunc{},
	}
}

// registers a handler func in the executor
func (e *Executor) Register(kind string, fn HandlerFunc) {
	e.handlers[kind] = fn
}

// spawns a go routine to execute a job and call onDone() when finished
func (e *Executor) Submit(ctx context.Context, job Job, onDone func()) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer onDone()
		e.runJob(ctx, job)
	}()
}

func (e *Executor) runJob(ctx context.Context, job Job) {
	// find job's handler
	handler, ok := e.handlers[job.Kind]
	if !ok {
		e.store.DeadLetter(ctx, job.ID, e.cfg.WorkerID, AttemptError{
			Attempt:  job.Attempts,
			Error:    fmt.Sprintf("no handler registered for kind %q", job.Kind),
			FailedAt: time.Now(),
		})
		return
	}

	// create a new context with job's timeout
	jobCtx, cancelJob := context.WithTimeout(ctx, job.Timeout)
	defer cancelJob()

	heartbeatCtx, stopHearbeat := context.WithCancel(ctx)

	// start heartbeat goroutine to extend lease
	go e.startHeartbeat(heartbeatCtx, job, cancelJob)

	// execute the job's handler
	err := e.executeSafe(jobCtx, handler, job)

	// stop heartbeat goroutine
	stopHearbeat()

	// handle job's state transition based on error returned
	if err == nil {
		_ = e.store.Complete(ctx, job.ID, e.cfg.WorkerID)
		return
	}

	var snoozeErr *SnoozeError
	if errors.As(err, &snoozeErr) {
		nextRunAt := time.Now().Add(snoozeErr.Duration)
		_ = e.store.Snooze(ctx, job.ID, e.cfg.WorkerID, nextRunAt)
		return
	}

	attemptErr := AttemptError{
		Attempt:  job.Attempts,
		Error:    err.Error(),
		FailedAt: time.Now(),
	}

	var nonRetryableError *NonRetryableError
	if errors.As(err, &nonRetryableError) || job.Attempts >= job.MaxAttempts {
		_ = e.store.DeadLetter(ctx, job.ID, e.cfg.WorkerID, attemptErr)
		return
	}

	nextRunAt := NextRetryAt(time.Now(), job.Attempts, e.cfg.RetryBaseDelay, e.cfg.RetryMaxDelay)
	_ = e.store.Retry(ctx, job.ID, e.cfg.WorkerID, attemptErr, nextRunAt)
}

// execute the handler with panic recovery
func (e *Executor) executeSafe(ctx context.Context, handler HandlerFunc, job Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in job handler: %v", r)
		}
	}()
	return handler(ctx, job)
}

// extends lease of a job for a worker at fixed intervals
func (e *Executor) startHeartbeat(ctx context.Context, job Job, cancelJob context.CancelFunc) {
	interval := e.cfg.LeaseDuration / 3
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := e.store.Heartbeat(ctx, job.ID, e.cfg.WorkerID, e.cfg.LeaseDuration)
			// if the current worker lost lease of this job, cancel job execution
			if errors.Is(err, ErrLeaseLost) {
				cancelJob()
				return
			}
		}
	}
}

func (e *Executor) Wait() {
	e.wg.Wait()
}
