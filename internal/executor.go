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
	jobCtx, cancel := context.WithTimeout(ctx, job.Timeout)
	defer cancel()

	// execute the job's handler
	err := e.executeSafe(jobCtx, handler, job)

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

func (e *Executor) Wait() {
	e.wg.Wait()
}
