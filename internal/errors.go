package internal

import (
	"errors"
	"fmt"
	"time"
)

type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string {
	if e.Err == nil {
        return "non-retryable job error"
    }
	return e.Err.Error()
}

func (e *NonRetryableError) Unwrap() error {
	return e.Err
}

type SnoozeError struct {
	Duration time.Duration
}

func (e *SnoozeError) Error() string {
	return fmt.Sprintf("job snoozed for %s", e.Duration)
}

var ErrDuplicateJob = errors.New("job already exists for this unique_key")
var ErrLeaseLost = errors.New("lease lost by worker")