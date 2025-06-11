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
	l.deltaSinceLastPop.Add(incrementInNano)
	return true
}

func (l *ResetBasedLimiter) AllowN(n int, replenishPerSecond float64, burst int) bool {
	return l.allowN(n, replenishPerSecond, burst, true)
}

func (l *ResetBasedLimiter) ForceN(n int, replenishPerSecond float64, burst int) bool {
	return l.allowN(n, replenishPerSecond, burst, false)
}

func (l *ResetBasedLimiter) IncrementResetAtBy(inc int64) {
	l.resetAt.Add(inc)
}

func (l *ResetBasedLimiter) GetResetAt() int64 {
	return l.resetAt.Load()
}

func (l *ResetBasedLimiter) PopResetAtDelta() int64 {
	return l.deltaSinceLastPop.Swap(0)
}
