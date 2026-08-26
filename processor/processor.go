package processor

import (
	"context"
	"fmt"
	"time"
)

// Processor accepts events and schedules handler calls according to Config.
//
// Replace the starter implementation with a concurrency-safe solution. You
// may add private fields, helper types, files, and tests, but keep the exported
// API unchanged.
type Processor struct {
	handler Handler
	config  Config
}

func New(handler Handler, config Config) (*Processor, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: handler is nil", ErrInvalidConfig)
	}
	if config.Workers <= 0 {
		return nil, fmt.Errorf("%w: workers must be positive", ErrInvalidConfig)
	}
	if config.QueueCapacity <= 0 {
		return nil, fmt.Errorf("%w: queue capacity must be positive", ErrInvalidConfig)
	}
	if config.MaxAttempts <= 0 {
		return nil, fmt.Errorf("%w: max attempts must be positive", ErrInvalidConfig)
	}
	if config.Backoff == nil {
		config.Backoff = func(int) time.Duration { return 0 }
	}

	return &Processor{handler: handler, config: config}, nil
}

func (p *Processor) Submit(context.Context, Event) error {
	return ErrNotImplemented
}

func (p *Processor) Close(context.Context) error {
	return ErrNotImplemented
}

func (p *Processor) Stats() Stats {
	return Stats{}
}
