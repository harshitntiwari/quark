package internal

import (
	"math"
	"math/rand/v2"
	"time"
)

// Computes the next run_at timestamp for a failed job using
// exponential backoff with jitter based on the attempt number (1-based).
func NextRetryAt(now time.Time, attempt int, baseDelay, maxDelay time.Duration) time.Time {
	if attempt < 1 {
    	attempt = 1
	}
	backOff := float64(baseDelay) * math.Pow(2, float64(attempt) - 1)
	cappedBackOff := math.Min(float64(maxDelay), backOff) 
	jitter := time.Duration(rand.N(int64(1 * time.Second)))

	delay := time.Duration(cappedBackOff) + jitter

	return now.Add(delay)
}