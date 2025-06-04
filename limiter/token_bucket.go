package limiter

import (
	"math"
	"sync"
	"time"
)

type Bucket struct {
	B         float64
	R         float64
	remaining float64
	lastCheck time.Time
	mu        sync.Mutex
}

func NewBucket() *Bucket {
	return &Bucket{
		remaining: math.MaxFloat64,
		lastCheck: time.Now(),
	}
}

func (b *Bucket) ForceN(n int, replenishPerSecond float64, burst int) bool {
	return b.allowN(n, replenishPerSecond, burst, false)
}

func (b *Bucket) AllowN(n int, replenishPerSecond float64, burst int) bool {
	return b.allowN(n, replenishPerSecond, burst, true)
}

func (b *Bucket) allowN(n int, replenishPerSecond float64, burst int, shouldCheck bool) bool {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	leak := now.Sub(b.lastCheck).Seconds() * replenishPerSecond

	if leak > 0 {
		if b.remaining+leak > float64(burst) {
			b.remaining = float64(burst)
		} else {
			b.remaining += leak
		}
		b.lastCheck = now
	}
	nFloat := float64(n)
	if !shouldCheck || b.remaining >= nFloat {
		b.remaining -= nFloat
		return true
	} else {
		return false
	}
}
