package limiter

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type ResetBasedLimiter struct {
	resetAt    atomic.Int64
	mu         sync.RWMutex
	deltaSince atomic.Int64
}

var _ Limiter = &ResetBasedLimiter{}

func NewResetbasedLimiter() *ResetBasedLimiter {
	l := &ResetBasedLimiter{}
	l.resetAt.Store(0)
	return l
}

func (l *ResetBasedLimiter) allowN(n int, replenishPerSecond float64, burst int, shouldCheck bool) bool {
	now := time.Now().UnixNano()
	if (shouldCheck && l.resetAt.Load() > now) || n > burst {
		return false
	}

	nanosecondsPerToken := int64(float64(time.Second) / replenishPerSecond)
	burstInNano := int64(burst) * nanosecondsPerToken

	incrementInNano := int64(n) * nanosecondsPerToken
	l.mu.Lock()
	defer l.mu.Unlock()
	newResetAt := math.Max(float64(l.resetAt.Load()), float64(now-burstInNano)) + float64(incrementInNano)
	if shouldCheck && newResetAt > float64(now) {
		return false
	}
	l.resetAt.Store(int64(newResetAt))
	l.deltaSince.Add(incrementInNano)
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
	return l.deltaSince.Swap(0)
}
