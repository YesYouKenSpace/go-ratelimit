package limiter

import (
	"time"

	"golang.org/x/time/rate"
)

type BuiltinLimiter struct {
	*rate.Limiter
}

func NewBuiltinLimiter(replenishPerSecond float64, burst int) *BuiltinLimiter {
	return &BuiltinLimiter{
		Limiter: rate.NewLimiter(rate.Limit(replenishPerSecond), burst),
	}
}

func (d *BuiltinLimiter) AllowN(n int, replenishPerSecond float64, burst int) bool {
	d.setRate(replenishPerSecond, burst)
	return d.Limiter.AllowN(time.Now(), n)
}

func (d *BuiltinLimiter) ForceN(n int, replenishPerSecond float64, burst int) bool {
	d.setRate(replenishPerSecond, burst)
	return d.Limiter.ReserveN(time.Now(), n).OK()
}

func (d *BuiltinLimiter) setRate(limit float64, burst int) bool {
	modified := false
	if d.Limiter.Burst() != burst {
		d.Limiter.SetBurst(burst)
		modified = true
	}
	if d.Limiter.Limit() != rate.Limit(limit) {
		d.Limiter.SetLimit(rate.Limit(limit))
		modified = true
	}
	return modified
}
