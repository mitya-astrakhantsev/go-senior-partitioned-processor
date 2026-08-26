// Package processor contains a bounded concurrent event processor.
package processor

import (
	"context"
	"errors"
	"time"
)

var (
	ErrClosed         = errors.New("processor is closed")
	ErrInvalidEvent   = errors.New("event id and partition key must be non-empty")
	ErrInvalidConfig  = errors.New("invalid processor config")
	ErrNotImplemented = errors.New("processor is not implemented")
)

// Event is an immutable input after Submit returns successfully.
type Event struct {
	ID           string
	PartitionKey string
	Payload      []byte
}

// Handler performs one delivery attempt.
// Implementations must respect ctx cancellation.
type Handler interface {
	Handle(ctx context.Context, event Event) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, Event) error

func (f HandlerFunc) Handle(ctx context.Context, event Event) error {
	return f(ctx, event)
}

// BackoffFunc returns the delay before the next attempt. The argument is the
// number of the attempt that has just failed, starting with 1.
type BackoffFunc func(attempt int) time.Duration

type Config struct {
	Workers       int
	QueueCapacity int
	MaxAttempts   int
	Backoff       BackoffFunc
}

type Stats struct {
	Accepted  uint64
	Succeeded uint64
	Failed    uint64
	Retried   uint64
	InFlight  uint64
}

// Permanent marks an error as non-retryable.
// Passing nil returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether err or one of its wrapped errors was marked
// with Permanent.
func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

type permanentError struct {
	err error
}

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }
