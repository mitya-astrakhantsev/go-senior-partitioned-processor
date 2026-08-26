package processor_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mitya-astrakhantsev/go-senior-partitioned-processor/processor"
)

func TestNewValidatesConfig(t *testing.T) {
	t.Parallel()

	validHandler := processor.HandlerFunc(func(context.Context, processor.Event) error { return nil })
	validConfig := processor.Config{Workers: 1, QueueCapacity: 1, MaxAttempts: 1}

	tests := []struct {
		name    string
		handler processor.Handler
		config  processor.Config
	}{
		{name: "nil handler", handler: nil, config: validConfig},
		{name: "zero workers", handler: validHandler, config: processor.Config{QueueCapacity: 1, MaxAttempts: 1}},
		{name: "zero queue", handler: validHandler, config: processor.Config{Workers: 1, MaxAttempts: 1}},
		{name: "zero attempts", handler: validHandler, config: processor.Config{Workers: 1, QueueCapacity: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := processor.New(tt.handler, tt.config)
			if !errors.Is(err, processor.ErrInvalidConfig) {
				t.Fatalf("New() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestProcessorPreservesOrderWithinPartition(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var got []string
	handler := processor.HandlerFunc(func(_ context.Context, event processor.Event) error {
		mu.Lock()
		got = append(got, event.ID)
		mu.Unlock()
		return nil
	})

	p := newProcessor(t, handler, processor.Config{Workers: 4, QueueCapacity: 8, MaxAttempts: 1})
	for _, id := range []string{"first", "second", "third"} {
		if err := p.Submit(context.Background(), processor.Event{ID: id, PartitionKey: "order-42"}); err != nil {
			t.Fatalf("Submit(%q): %v", id, err)
		}
	}
	closeProcessor(t, p)

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"first", "second", "third"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("handler order = %v, want %v", got, want)
	}
}

func TestProcessorRunsDifferentPartitionsConcurrently(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	started := make(chan struct{}, 2)
	var running atomic.Int32
	var maxRunning atomic.Int32

	handler := processor.HandlerFunc(func(ctx context.Context, _ processor.Event) error {
		current := running.Add(1)
		defer running.Add(-1)
		for {
			maximum := maxRunning.Load()
			if current <= maximum || maxRunning.CompareAndSwap(maximum, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	p := newProcessor(t, handler, processor.Config{Workers: 2, QueueCapacity: 2, MaxAttempts: 1})
	if err := p.Submit(context.Background(), processor.Event{ID: "a", PartitionKey: "A"}); err != nil {
		t.Fatal(err)
	}
	if err := p.Submit(context.Background(), processor.Event{ID: "b", PartitionKey: "B"}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("events from different partitions did not start concurrently")
		}
	}
	close(release)
	closeProcessor(t, p)

	if got := maxRunning.Load(); got != 2 {
		t.Fatalf("maximum concurrency = %d, want 2", got)
	}
}

func TestProcessorRetriesAndRecognizesPermanentErrors(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	attempts := map[string]int{}
	handler := processor.HandlerFunc(func(_ context.Context, event processor.Event) error {
		mu.Lock()
		defer mu.Unlock()
		attempts[event.ID]++
		switch event.ID {
		case "eventual-success":
			if attempts[event.ID] < 3 {
				return errors.New("temporary")
			}
			return nil
		case "permanent-failure":
			return processor.Permanent(errors.New("invalid payload"))
		default:
			return nil
		}
	})

	p := newProcessor(t, handler, processor.Config{
		Workers:       2,
		QueueCapacity: 2,
		MaxAttempts:   3,
		Backoff:       func(int) time.Duration { return 0 },
	})
	for _, event := range []processor.Event{
		{ID: "eventual-success", PartitionKey: "A"},
		{ID: "permanent-failure", PartitionKey: "B"},
	} {
		if err := p.Submit(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	closeProcessor(t, p)

	mu.Lock()
	defer mu.Unlock()
	if attempts["eventual-success"] != 3 || attempts["permanent-failure"] != 1 {
		t.Fatalf("attempts = %v", attempts)
	}
	if got, want := p.Stats(), (processor.Stats{Accepted: 2, Succeeded: 1, Failed: 1, Retried: 2}); got != want {
		t.Fatalf("Stats() = %+v, want %+v", got, want)
	}
}

func TestProcessorDeduplicatesAndOwnsPayload(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	received := make(chan string, 1)
	readPayload := make(chan struct{})
	handler := processor.HandlerFunc(func(_ context.Context, event processor.Event) error {
		calls.Add(1)
		<-readPayload
		received <- string(event.Payload)
		return nil
	})

	p := newProcessor(t, handler, processor.Config{Workers: 1, QueueCapacity: 2, MaxAttempts: 1})
	payload := []byte("original")
	event := processor.Event{ID: "same-id", PartitionKey: "A", Payload: payload}
	if err := p.Submit(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	copy(payload, "mutated!")
	close(readPayload)
	if err := p.Submit(context.Background(), processor.Event{ID: "same-id", PartitionKey: "B"}); err != nil {
		t.Fatal(err)
	}
	closeProcessor(t, p)

	if got := <-received; got != "original" {
		t.Fatalf("handler payload = %q, want original", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestSubmitHonorsBackpressureAndContext(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	started := make(chan struct{})
	handler := processor.HandlerFunc(func(context.Context, processor.Event) error {
		close(started)
		<-release
		return nil
	})

	p := newProcessor(t, handler, processor.Config{Workers: 1, QueueCapacity: 1, MaxAttempts: 1})
	if err := p.Submit(context.Background(), processor.Event{ID: "occupies-capacity", PartitionKey: "A"}); err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := p.Submit(ctx, processor.Event{ID: "must-wait", PartitionKey: "B"})
	if !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("Submit() error = %v, want DeadlineExceeded", err)
	}

	close(release)
	closeProcessor(t, p)
}

func TestCloseRejectsNewEventsAndCanBeCalledAgain(t *testing.T) {
	t.Parallel()

	p := newProcessor(t, processor.HandlerFunc(func(context.Context, processor.Event) error { return nil }), processor.Config{
		Workers: 1, QueueCapacity: 1, MaxAttempts: 1,
	})
	closeProcessor(t, p)

	if err := p.Submit(context.Background(), processor.Event{ID: "late", PartitionKey: "A"}); !errors.Is(err, processor.ErrClosed) {
		t.Fatalf("Submit() after Close = %v, want ErrClosed", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}
}

func newProcessor(t *testing.T, handler processor.Handler, config processor.Config) *processor.Processor {
	t.Helper()
	p, err := processor.New(handler, config)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return p
}

func closeProcessor(t *testing.T, p *processor.Processor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func ExamplePermanent() {
	err := processor.Permanent(errors.New("validation failed"))
	fmt.Println(processor.IsPermanent(err))
	// Output: true
}
