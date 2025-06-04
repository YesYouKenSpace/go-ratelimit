package ratelimit

import (
	"sync"

	"github.com/yesyoukenspace/go-ratelimit/limiter"
)

type Mutex[Limiter limiter.Limiter] struct {
	mu           sync.Mutex
	limiters     map[string]Limiter
	newLimiterFn func() Limiter
}

var _ Ratelimiter = &Mutex[limiter.Limiter]{}

func NewMutex[Limiter limiter.Limiter](newLimiterFn func() Limiter) *Mutex[Limiter] {
	return &Mutex[Limiter]{
		limiters:     make(map[string]Limiter),
		newLimiterFn: newLimiterFn,
	}
}

func (d *Mutex[Limiter]) getLimiter(key string) Limiter {
	d.mu.Lock()
	defer d.mu.Unlock()
	l, ok := d.limiters[key]
	if !ok {
		l = d.newLimiterFn()
		d.limiters[key] = l
	}
	return l
}

func (d *Mutex[Limiter]) AllowN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	l := d.getLimiter(key)
	return l.AllowN(cost, replenishPerSecond, burst), nil
}

type RWMutex[Limiter limiter.Limiter] struct {
	mu           sync.RWMutex
	limiters     map[string]Limiter
	newLimiterFn func() Limiter
}

func NewRWMutex[Limiter limiter.Limiter](newLimiterFn func() Limiter) *RWMutex[Limiter] {
	return &RWMutex[Limiter]{
		limiters:     make(map[string]Limiter),
		newLimiterFn: newLimiterFn,
	}
}

func (d *RWMutex[Limiter]) getLimiter(key string) limiter.Limiter {
	d.mu.RLock()
	l, ok := d.limiters[key]
	d.mu.RUnlock()
	if !ok {
		d.mu.Lock()
		defer d.mu.Unlock()
		l = d.newLimiterFn()
		d.limiters[key] = l
	}
	return l
}

func (d *RWMutex[Limiter]) AllowN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	l := d.getLimiter(key)
	return l.AllowN(cost, replenishPerSecond, burst), nil
}
