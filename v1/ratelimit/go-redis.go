package ratelimit

import (
	"context"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

type GoRedisRate struct {
	ctx     context.Context
	limiter *redis_rate.Limiter
}

var _ Ratelimiter = &GoRedisRate{}

func (d *GoRedisRate) AllowN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	// TODO: rate here only works for more than 1 rps, allow for less than 1 rps, and integers only
	res, err := d.limiter.AllowN(d.ctx, key, redis_rate.Limit{Rate: int(replenishPerSecond), Burst: burst, Period: time.Second}, cost)
	if err != nil {
		return false, err
	}
	return res.Allowed > 0, nil
}

func NewGoRedis(redisClient *redis.Client) *GoRedisRate {
	return &GoRedisRate{
		ctx:     context.Background(),
		limiter: redis_rate.NewLimiter(redisClient),
	}
}
