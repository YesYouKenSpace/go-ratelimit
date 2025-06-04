package ratelimit

import (
	"sync"

	"github.com/yesyoukenspace/go-ratelimit/limiter"
)

type SyncMapLoadThenLoadOrStore[Limiter limiter.Limiter] struct {
	limiters     sync.Map
	newLimiterFn func() Limiter
}

var _ Ratelimiter = &SyncMapLoadThenLoadOrStore[limiter.Limiter]{}

func NewSyncMapLoadThenLoadOrStore[Limiter limiter.Limiter](newLimiterFn func() Limiter) *SyncMapLoadThenLoadOrStore[Limiter] {
	return &SyncMapLoadThenLoadOrStore[Limiter]{
		newLimiterFn: newLimiterFn,
	}
}

func (d *SyncMapLoadThenLoadOrStore[Limiter]) AllowN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	l, ok := d.limiters.Load(key)
	// if key is not found, then create a new limiter and store it
	// reduces allocation by doing this
	if !ok {
		l, _ = d.limiters.LoadOrStore(key, d.newLimiterFn())
	}
	return l.(limiter.Limiter).AllowN(cost, replenishPerSecond, burst), nil
}

func (d *SyncMapLoadThenLoadOrStore[Limiter]) ForceN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	l, ok := d.limiters.Load(key)
	// if key is not found, then create a new limiter and store it
	// reduces allocation by doing this
	if !ok {
		l, _ = d.limiters.LoadOrStore(key, d.newLimiterFn())
	}
	return l.(limiter.Limiter).ForceN(cost, replenishPerSecond, burst), nil
}

type SyncMapLoadOrStore[Limiter limiter.Limiter] struct {
	limiters     sync.Map
	newLimiterFn func() Limiter
}

func NewSyncMapLoadOrStore[Limiter limiter.Limiter](newLimiterFn func() Limiter) *SyncMapLoadOrStore[Limiter] {
	return &SyncMapLoadOrStore[Limiter]{
		newLimiterFn: newLimiterFn,
	}
}

func (d *SyncMapLoadOrStore[Limiter]) AllowN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	l, _ := d.limiters.LoadOrStore(key, d.newLimiterFn())
	return l.(limiter.Limiter).AllowN(cost, replenishPerSecond, burst), nil
}

var _ Ratelimiter = &SyncMapLoadOrStore[limiter.Limiter]{}

type SyncMapLoadThenStore[Limiter limiter.Limiter] struct {
	limiters     sync.Map
	newLimiterFn func() Limiter
}

var _ Ratelimiter = &SyncMapLoadThenStore[limiter.Limiter]{}

func NewSyncMapLoadThenStore[Limiter limiter.Limiter](newLimiterFn func() Limiter) *SyncMapLoadThenStore[Limiter] {
	return &SyncMapLoadThenStore[Limiter]{
		limiters:     sync.Map{},
		newLimiterFn: newLimiterFn,
	}
}

func (r *SyncMapLoadThenStore[Limiter]) AllowN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	l, ok := r.limiters.Load(key)
	if !ok {
		l = r.newLimiterFn()
		// note: We might overwrite the limiter if multiple goroutines are trying to create the limiter at the same time, it is not a big deal as we may just have a little inaccurate rate limiting for a short period of time
		r.limiters.Store(key, l)
	}
	return l.(Limiter).AllowN(cost, replenishPerSecond, burst), nil
}

func (r *SyncMapLoadThenStore[Limiter]) GetLimiter(key string) Limiter {
	l, ok := r.limiters.Load(key)
	if !ok {
		l = limiter.NewResetbasedLimiter()
		r.limiters.Store(key, l)
	}
	return l.(Limiter)
}
