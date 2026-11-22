package ratelimit

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"time"
)

// Limiter limita o volume de dados transferidos em bytes por segundo.
//
// O limite usa um token bucket: uma rajada inicial de até burst bytes é
// permitida e os tokens são repostos continuamente a partir da taxa.
type Limiter struct {
	mu      sync.Mutex
	rate    float64
	burst   float64
	tokens  float64
	last    time.Time
	now     func() time.Time
	changed chan struct{}
}

// New cria um limitador com a taxa informada em bytes por segundo.
// O argumento opcional burst define o tamanho da rajada; por padrão, uma
// segunda de transferência cabe no bucket.
func New(bytesPerSecond int64, burst ...int64) *Limiter {
	rate := bytesPerSecond
	burstSize := rate
	if len(burst) > 0 {
		burstSize = burst[0]
	}
	return NewLimiter(rate, burstSize)
}

// NewLimiter cria um limitador em bytes por segundo.
func NewLimiter(bytesPerSecond int64, burstSize ...int64) *Limiter {
	rate := float64(bytesPerSecond)
	burstValue := rate
	if len(burstSize) > 0 {
		burstValue = float64(burstSize[0])
	}
	if burstValue < 0 {
		burstValue = 0
	}
	if bytesPerSecond <= 0 {
		rate = 0
		burstValue = 0
	}
	if burstValue < 1 && rate > 0 {
		burstValue = 1
	}

	return &Limiter{
		rate:    rate,
		burst:   burstValue,
		tokens:  burstValue,
		last:    time.Now(),
		now:     time.Now,
		changed: make(chan struct{}),
	}
}

// SetRate altera a taxa. Se a nova taxa for menor ou igual a zero, o
// limitador deixa de restringir a transferência.
func (l *Limiter) SetRate(bytesPerSecond int64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refillLocked(l.now())
	l.rate = float64(bytesPerSecond)
	if l.rate < 0 {
		l.rate = 0
	}
	if l.rate == 0 {
		l.burst = 0
		l.tokens = 0
	} else if l.burst <= 0 {
		l.burst = l.rate
		l.tokens = l.burst
	}
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.signalChangeLocked()
}

// SetBurst altera o tamanho máximo da rajada permitida.
func (l *Limiter) SetBurst(burst int64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refillLocked(l.now())
	if burst < 0 {
		burst = 0
	}
	l.burst = float64(burst)
	if l.burst < 1 && l.rate > 0 {
		l.burst = 1
	}
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.signalChangeLocked()
}

func (l *Limiter) signalChangeLocked() {
	if l.changed == nil {
		return
	}
	close(l.changed)
	l.changed = make(chan struct{})
}

// BytesPerSecond retorna a taxa configurada.
func (l *Limiter) BytesPerSecond() int64 {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return int64(l.rate)
}

func (l *Limiter) refillLocked(now time.Time) {
	if l.rate <= 0 {
		l.last = now
		return
	}
	elapsed := now.Sub(l.last).Seconds()
	if elapsed > 0 {
		l.tokens += elapsed * l.rate
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		l.last = now
	}
}

// Allow informa se n bytes podem ser consumidos imediatamente.
func (l *Limiter) Allow(n int64) bool {
	if n <= 0 || l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rate <= 0 {
		return true
	}
	l.refillLocked(l.now())
	needed := float64(n)
	if needed > l.burst {
		return false
	}
	if l.tokens < needed {
		return false
	}
	l.tokens -= needed
	return true
}

// AllowN é um alias de Allow que usa int.
func (l *Limiter) AllowN(n int) bool {
	return l.Allow(int64(n))
}

// WaitContext bloqueia até que n bytes possam ser enviados ou o contexto seja cancelado.
func (l *Limiter) WaitContext(ctx context.Context, n int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if n <= 0 || l == nil {
		return nil
	}

	remaining := int64(n)
	var consumed int64
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			l.refund(consumed)
			return err
		}

		l.mu.Lock()
		if l.changed == nil {
			l.changed = make(chan struct{})
		}
		if l.rate <= 0 {
			l.mu.Unlock()
			return nil
		}

		now := l.now()
		l.refillLocked(now)
		chunk := remaining
		if float64(chunk) > l.burst {
			chunk = int64(l.burst)
		}

		needed := float64(chunk)
		waitSeconds := 0.0
		if l.tokens < needed {
			waitSeconds = (needed - l.tokens) / l.rate
		} else {
			l.tokens -= needed
			l.mu.Unlock()
			remaining -= chunk
			consumed += chunk
			continue
		}
		changed := l.changed
		l.mu.Unlock()

		timer := time.NewTimer(safeWaitDuration(waitSeconds))
		select {
		case <-ctx.Done():
			stopTimer(timer)
			l.refund(consumed)
			return ctx.Err()
		case <-timer.C:
		case <-changed:
			stopTimer(timer)
		}
	}
	return nil
}

func safeWaitDuration(seconds float64) time.Duration {
	if math.IsNaN(seconds) || seconds <= 0 {
		return time.Nanosecond
	}

	maxDuration := time.Duration(1<<63 - 1)
	maxSeconds := float64(maxDuration) / float64(time.Second)
	if math.IsInf(seconds, 1) || seconds >= maxSeconds {
		return maxDuration
	}

	nanoseconds := math.Ceil(seconds * float64(time.Second))
	if nanoseconds >= float64(maxDuration) {
		return maxDuration
	}
	if nanoseconds < 1 {
		return time.Nanosecond
	}
	return time.Duration(nanoseconds)
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (l *Limiter) refund(n int64) {
	if l == nil || n <= 0 {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rate <= 0 {
		return
	}
	l.refillLocked(l.now())
	l.tokens += float64(n)
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
}

// Wait aceita Wait(n) ou Wait(ctx, n). A forma flexível evita ambiguidade
// entre os estilos de uso comuns.
func (l *Limiter) Wait(args ...any) error {
	switch len(args) {
	case 1:
		n, ok := args[0].(int)
		if !ok {
			return context.DeadlineExceeded
		}
		return l.WaitContext(context.Background(), n)
	case 2:
		ctx, ok := args[0].(context.Context)
		if !ok {
			return context.DeadlineExceeded
		}
		n, ok := args[1].(int)
		if !ok {
			return context.DeadlineExceeded
		}
		return l.WaitContext(ctx, n)
	default:
		return context.DeadlineExceeded
	}
}

// WaitN aceita WaitN(n) ou WaitN(ctx, n).
func (l *Limiter) WaitN(args ...any) error {
	return l.Wait(args...)
}

// LimitedReader aplica o limite antes das leituras.
type LimitedReader struct {
	r       io.Reader
	limiter *Limiter
	ctx     context.Context
}

func (lr *LimitedReader) Read(p []byte) (int, error) {
	if lr == nil || lr.r == nil {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	ctx := lr.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := lr.limiter.WaitContext(ctx, len(p)); err != nil {
		return 0, err
	}

	n, err := lr.r.Read(p)
	if n < 0 || n > len(p) {
		lr.limiter.refund(int64(len(p)))
		return 0, errors.New("reader returned an invalid byte count")
	}
	lr.limiter.refund(int64(len(p) - n))
	return n, err
}

// LimitReader envolve r com este limitador.
func (l *Limiter) LimitReader(r io.Reader) io.Reader {
	return &LimitedReader{r: r, limiter: l, ctx: context.Background()}
}

// Reader é um alias conveniente de LimitReader.
func (l *Limiter) Reader(r io.Reader) io.Reader {
	return l.LimitReader(r)
}

// LimitedWriter aplica o limite antes das escritas.
type LimitedWriter struct {
	w       io.Writer
	limiter *Limiter
	ctx     context.Context
}

func (lw *LimitedWriter) Write(p []byte) (int, error) {
	if lw == nil || lw.w == nil {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	ctx := lw.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := lw.limiter.WaitContext(ctx, len(p)); err != nil {
		return 0, err
	}

	n, err := lw.w.Write(p)
	if n < 0 || n > len(p) {
		lw.limiter.refund(int64(len(p)))
		return 0, errors.New("writer returned an invalid byte count")
	}
	lw.limiter.refund(int64(len(p) - n))
	return n, err
}

// LimitWriter envolve w com este limitador.
func (l *Limiter) LimitWriter(w io.Writer) io.Writer {
	return &LimitedWriter{w: w, limiter: l, ctx: context.Background()}
}

// Writer é um alias conveniente de LimitWriter.
func (l *Limiter) Writer(w io.Writer) io.Writer {
	return l.LimitWriter(w)
}

// Construtores independentes para uso direto.
func NewLimitedReader(r io.Reader, limiter *Limiter) io.Reader {
	return limiter.LimitReader(r)
}

func NewLimitedWriter(w io.Writer, limiter *Limiter) io.Writer {
	return limiter.LimitWriter(w)
}
