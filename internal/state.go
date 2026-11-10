package internal

import (
	"errors"
	"fmt"
	"slices"
)

type JobState string

const (
	Available JobState = "available"
	Running   JobState = "running"
	Completed JobState = "completed"
	Dead      JobState = "dead"
	Cancelled JobState = "cancelled"
)

func (s JobState) IsValid() bool {
	switch s {
	case Available, Running, Completed, Dead, Cancelled:
		return true
	}
	return false
}

func (s JobState) IsTerminal() bool {
	switch s {
	case Completed, Dead, Cancelled:
		return true
	}
	return false
}

var transitionMap = map[JobState][]JobState{
	Available: {Running, Cancelled},
	Running:   {Completed, Cancelled, Available, Dead},
	Dead:      {Available, Cancelled},
}

var ErrInvalidTransition = errors.New("invalid job state transition")

func CanTransition(from, to JobState) bool {
	allowed, ok := transitionMap[from]
	if !ok {
		return false
	}
	return slices.Contains(allowed, to)
}

func ValidateTransition(from, to JobState) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("invalid state transition from %q to %q: %w", from, to, ErrInvalidTransition)
	}
	return nil
}
