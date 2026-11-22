package ratelimit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func freezeClock(l *Limiter) func(time.Duration) {
	now := time.Now()
	l.mu.Lock()
	l.last = now
	l.now = func() time.Time { return now }
	l.mu.Unlock()

	return func(d time.Duration) {
		now = now.Add(d)
	}
}

func TestWaitContextRequestLargerThanBurst(t *testing.T) {
	l := NewLimiter(10, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := l.WaitContext(ctx, 20)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitContext returned %v; want context deadline exceeded", err)
	}

	if !l.Allow(10) {
		t.Fatal("canceled WaitContext did not restore its reserved tokens")
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return 1, nil
}

func TestLimitedWriterRefundsUnusedTokens(t *testing.T) {
	l := NewLimiter(1, 4)
	freezeClock(l)
	w := l.LimitWriter(shortWriter{})

	n, err := w.Write([]byte("data"))
	if err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if n != 1 {
		t.Fatalf("Write returned %d bytes; want 1", n)
	}

	if !l.Allow(3) {
		t.Fatal("tokens for the three unwritten bytes were not refunded")
	}
}

func TestWaitContextWakesWhenRateIsDisabled(t *testing.T) {
	l := NewLimiter(1, 1)
	if !l.Allow(1) {
		t.Fatal("failed to consume initial burst")
	}

	waitStarted := make(chan struct{})
	var once sync.Once
	l.mu.Lock()
	l.now = func() time.Time {
		once.Do(func() { close(waitStarted) })
		return time.Now()
	}
	l.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- l.WaitContext(ctx, 1)
	}()

	select {
	case <-waitStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("WaitContext did not start waiting")
	}

	l.SetRate(0)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitContext returned error after disabling rate: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("WaitContext was not woken when the rate was disabled")
	}
}

func TestAllowBurstAndRefill(t *testing.T) {
	l := NewLimiter(10, 10)
	advance := freezeClock(l)

	if !l.Allow(10) {
		t.Fatal("initial burst should be available")
	}
	if l.Allow(1) {
		t.Fatal("Allow succeeded after the bucket was exhausted")
	}

	advance(100 * time.Millisecond)
	if !l.Allow(1) {
		t.Fatal("one byte should be available after 100ms at 10 bytes/s")
	}
	if !l.Allow(0) {
		t.Fatal("Allow(0) should succeed without consuming tokens")
	}
}
