package limiter

import (
	"sync"
	"sync/atomic"
	"time"
)

type ResetBasedLimiter struct {
	resetAt           atomic.Int64
	mu                sync.RWMutex
	deltaSinceLastPop atomic.Int64
}

var _ Limiter = &ResetBasedLimiter{}

func NewResetbasedLimiter() *ResetBasedLimiter {
	l := &ResetBasedLimiter{}
	return l
}

// Still in development, subject to changes, use at your own risks
func (l *ResetBasedLimiter) ReserveN(n int, replenishPerSecond float64, burst int) Reservation {
	now := time.Now().UnixNano()

	nanosecondsPerToken := int64(float64(time.Second) / replenishPerSecond)
	burstInNano := int64(burst) * nanosecondsPerToken

	incrementInNano := int64(n) * nanosecondsPerToken
	l.mu.Lock()
	defer l.mu.Unlock()
	newResetAt := max(now-burstInNano, l.resetAt.Load())
	newResetAt += incrementInNano
	l.resetAt.Store(newResetAt)
	l.AddDeltaSinceLastPop(incrementInNano)

	return &resetBasedLimiterReservation{
		timeToAct:       time.Unix(0, newResetAt),
		limiter:         l,
		incrementInNano: incrementInNano,
	}
}

func (l *ResetBasedLimiter) allowN(n int, replenishPerSecond float64, burst int, shouldCheck bool) bool {
	now := time.Now().UnixNano()
	if shouldCheck && (l.resetAt.Load() > now || n > burst) {
		return false
	}

	nanosecondsPerToken := int64(float64(time.Second) / replenishPerSecond)
	burstInNano := int64(burst) * nanosecondsPerToken

	incrementInNano := int64(n) * nanosecondsPerToken
	l.mu.Lock()
	defer l.mu.Unlock()
	newResetAt := max(now-burstInNano, l.resetAt.Load())
	newResetAt += incrementInNano
	if shouldCheck && newResetAt > now {
		return false
	}
	l.resetAt.Store(newResetAt)
	l.AddDeltaSinceLastPop(incrementInNano)
	return true
}

func (l *ResetBasedLimiter) AllowN(n int, replenishPerSecond float64, burst int) bool {
	return l.allowN(n, replenishPerSecond, burst, true)
}

func (l *ResetBasedLimiter) ForceN(n int, replenishPerSecond float64, burst int) bool {
	return l.allowN(n, replenishPerSecond, burst, false)
}

func (l *ResetBasedLimiter) IncrementResetAtBy(inc int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.resetAt.Add(inc)
}

func (l *ResetBasedLimiter) SetResetAt(resetAt int64) {
	l.resetAt.Store(resetAt)
}

func (l *ResetBasedLimiter) GetResetAt() int64 {
	return l.resetAt.Load()
}

func (l *ResetBasedLimiter) PopResetAtDelta() int64 {
	return l.deltaSinceLastPop.Swap(0)
}

func (l *ResetBasedLimiter) AddDeltaSinceLastPop(delta int64) {
	l.deltaSinceLastPop.Add(delta)
}

func (l *ResetBasedLimiter) GetUsage(replenishPerSecond float64, burst int) int64 {
	now := time.Now().UnixNano()
	resetAt := l.resetAt.Load()
	nanosecondsPerToken := int64(float64(time.Second) / replenishPerSecond)
	left := (now - resetAt) / nanosecondsPerToken
	burstInt64 := int64(burst)
	if left > burstInt64 {
		return burstInt64
	}
	return burstInt64 - left
}

type resetBasedLimiterReservation struct {
	timeToAct       time.Time
	limiter         *ResetBasedLimiter
	incrementInNano int64
}

func (r *resetBasedLimiterReservation) Cancel() {
	r.limiter.IncrementResetAtBy(-r.incrementInNano)
	r.limiter.AddDeltaSinceLastPop(-r.incrementInNano)
}
