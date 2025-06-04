package ratelimit

import (
	"github.com/yesyoukenspace/go-ratelimit/limiter"
)

type NewLimiterFn = func(rate float64, burst int) limiter.Limiter

type Ratelimiter interface {
	AllowN(string, int, float64, int) (bool, error)
}

func NewDefaultLimiter() limiter.Limiter {
	return limiter.NewResetbasedLimiter()
}
