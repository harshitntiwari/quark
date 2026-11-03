package quark

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/harshitntiwari/quark/internal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// package defaults for job-related configs
const (
	defaultQueue       = "default"
	defaultPriority    = 0
	defaultMaxAttempts = 3
	defaultTimeout     = 60 * time.Second
)

// ClientConfig configures default job parameters for a [Client].
// Zero values fall back to package defaults.
type ClientConfig struct {
	// DefaultQueue is the queue name used when EnqueueOpts.Queue is empty.
	// Defaults to "default".
	DefaultQueue       string

	// DefaultMaxAttempts is the maximum number of execution attempts before a
	// failing job is moved to the dead state. Defaults to 3.
	DefaultMaxAttempts int

	// DefaultTimeout is the maximum duration a single job attempt may run before
	// its context is cancelled. Defaults to 60s.
	DefaultTimeout     time.Duration
}

type Client struct {
	pool  *pgxpool.Pool
	store internal.Store
	cfg   ClientConfig
}

// NewClient creates and returns a new quark producer [Client]
func NewClient(pool *pgxpool.Pool, cfg *ClientConfig) (*Client, error) {
	var c ClientConfig
	if cfg != nil {
		c = *cfg
		if c.DefaultMaxAttempts < 0 {
			return nil, fmt.Errorf("default max attempts must be positive")
		}
		if c.DefaultTimeout < 0 {
			return nil, fmt.Errorf("default timeout must be positive")
		}
	}
	return &Client{
		pool:  pool,
		store: internal.NewDBStore(pool),
		cfg: ClientConfig{
			DefaultQueue:       cmp.Or(c.DefaultQueue, string(defaultQueue)),
			DefaultMaxAttempts: cmp.Or(c.DefaultMaxAttempts, int(defaultMaxAttempts)),
			DefaultTimeout:     cmp.Or(c.DefaultTimeout, defaultTimeout),
		},
	}, nil
}

// EnqueueOpts specifies per-job overrides when enqueuing a job.
// All fields are optional; zero values fall back to the Client's defaults.
type EnqueueOpts struct {
	// Queue overrides the target queue name for this job.
	Queue       string

	// Priority sets the execution priority (0 is highest priority; higher
	// numbers run later). Defaults to 0.
	Priority    int

	// MaxAttempts overrides the maximum number of attempts for this job.
	MaxAttempts int

	// RunAt schedules the job to become eligible for execution at a specific time.
	// Cannot be combined with Delay. Defaults to time.Now().
	RunAt       time.Time

	// Delay schedules the job to run after the given duration has elapsed.
	// Cannot be combined with RunAt.
	Delay       time.Duration

	// Timeout overrides the per-attempt execution timeout for this job.
	Timeout     time.Duration

	// UniqueKey deduplicates active jobs. If a job with the same UniqueKey is
	// currently in the 'available' or 'running' state, Enqueue returns ErrDuplicateJob.
	UniqueKey   string
}

type BatchItem struct {
	Args JobArgs
	Opts *EnqueueOpts
}

// Enqueue inserts a new job into the queue using the client's connection pool.
func (c *Client) Enqueue(ctx context.Context, args JobArgs, opts *EnqueueOpts) (Job, error) {
	return c.enqueue(ctx, c.pool, args, opts)
}

// EnqueueTx inserts a new job within the caller's existing database transaction.
func (c *Client) EnqueueTx(ctx context.Context, tx pgx.Tx, args JobArgs, opts *EnqueueOpts) (Job, error) {
	if tx == nil {
		return Job{}, fmt.Errorf("missing database transaction")
	}
	return c.enqueue(ctx, tx, args, opts)
}

// EnqueueBatch inserts multiple jobs in a single network round-trip using the client's connection pool.
func (c *Client) EnqueueBatch(ctx context.Context, items []BatchItem) ([]Job, error) {
	return c.enqueueBatch(ctx, c.pool, items)
}

// EnqueueBatchTx inserts multiple jobs in a single batch within the caller's existing database transaction.
func (c *Client) EnqueueBatchTx(ctx context.Context, tx pgx.Tx, items []BatchItem) ([]Job, error) {
		if tx == nil {
		return nil, fmt.Errorf("missing database transaction")
	}
	return c.enqueueBatch(ctx, tx, items) 
}

func (c *Client) enqueue(ctx context.Context, dt internal.DBTX, args JobArgs, opts *EnqueueOpts) (Job, error) {

	p, err := c.validateArgsAndOpts(args, opts)
	if err != nil {
		return Job{}, err
	}

	return c.store.Insert(ctx, dt, p)
}

func (c *Client) enqueueBatch(ctx context.Context, dt internal.DBTX, items []BatchItem)([]Job, error) {
	var params = make([]internal.InsertParams, 0, len(items))
	for _, i := range items {
		p, err := c.validateArgsAndOpts(i.Args, i.Opts)
		if err != nil {
			return nil, err
		}
		params = append(params, p)
	}

	return c.store.InsertBatch(ctx, dt, params)
}

func (c *Client) validateArgsAndOpts(args JobArgs, opts *EnqueueOpts) (internal.InsertParams, error) {
	// validate args
	if args == nil {
		return internal.InsertParams{}, fmt.Errorf("missing job args")
	}
	if args.Kind() == "" {
		return internal.InsertParams{}, fmt.Errorf("missing Kind in job args")
	}
	a, err := json.Marshal(args)
	if err != nil {
		return internal.InsertParams{}, err
	}

	// build InsertParams with defaults
	p := internal.InsertParams{
		Args:        a,
		Kind:        args.Kind(),
		Queue:       c.cfg.DefaultQueue,
		Priority:    defaultPriority,
		MaxAttempts: c.cfg.DefaultMaxAttempts,
		RunAt:       time.Now(),
		Timeout:     c.cfg.DefaultTimeout,
	}

	if opts != nil {
		// validate opts
		if opts.Priority < 0 {
			return internal.InsertParams{}, fmt.Errorf("priority must be a non-negative integer")
		}
		if opts.MaxAttempts < 0 {
			return internal.InsertParams{}, fmt.Errorf("max attempts must be a positive integer")
		}
		if opts.Timeout < 0 {
			return internal.InsertParams{}, fmt.Errorf("timeout must be positive")
		}
		if opts.Delay < 0 {
			return internal.InsertParams{}, fmt.Errorf("delay must be non-negative")
		}
		if !opts.RunAt.IsZero() && opts.Delay > 0 {
			return internal.InsertParams{}, fmt.Errorf("conflicting scheduling options configured: RunAt and Delay")
		}

		// override opts
		p.Queue = cmp.Or(opts.Queue, c.cfg.DefaultQueue)
		p.Priority = cmp.Or(opts.Priority, int(defaultPriority))
		p.MaxAttempts = cmp.Or(opts.MaxAttempts, c.cfg.DefaultMaxAttempts)
		p.Timeout = cmp.Or(opts.Timeout, c.cfg.DefaultTimeout)
		if !opts.RunAt.IsZero() {
			p.RunAt = opts.RunAt
		} else if opts.Delay > 0 {
			p.RunAt = time.Now().Add(opts.Delay)
		}
		if opts.UniqueKey != "" {
			p.UniqueKey = &opts.UniqueKey
		}
	}
	return p, nil
}
