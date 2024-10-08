package limiter

import (
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

func NewBucket(rate float64, burst int) *Bucket {
	return &Bucket{
		B:         float64(burst),
		remaining: float64(burst),
		R:         rate,
		lastCheck: time.Now(),
	}
}

func (b *Bucket) AllowN(n int) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	leak := now.Sub(b.lastCheck).Seconds() * b.R

	if leak > 0 {
		if b.remaining+leak > b.B {
			b.remaining = b.B
		} else {
			b.remaining += leak
		}
		b.lastCheck = now
	}
	nFloat := float64(n)
	if b.remaining >= nFloat {
		b.remaining -= nFloat
		return true, nil
	} else {
		return false, nil
	}
}

func (b *Bucket) Allow() (bool, error) {
	return b.AllowN(1)
}
