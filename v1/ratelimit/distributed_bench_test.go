package ratelimit

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"testing"

	"github.com/yesyoukenspace/go-ratelimit/internal/utils"
)

func BenchmarkDistributed(b *testing.B) {
	rate := math.Pow10(4)
	burst := int(math.Pow10(4))
	ratelimiters := []struct {
		name string
		c    func() Ratelimiter
	}{
		{name: "Redis", c: func() Ratelimiter { return NewGoRedis(newRDB(2)) }},
		{name: "Redis with delay in sync", c: func() Ratelimiter {
			return NewRedisDelayedSync(context.Background(), RedisDelayedSyncOption{
				RedisClient:  newRDB(3),
				SyncInterval: time.Second / 2,
			})
		},
		},
	}

	for _, concurrentUsers := range []int{2048, 16384} {
		for _, limiter := range ratelimiters {
			benchmarkDistributed(b, benchmarkDistributedConfig{
				name:                  limiter.name,
				parallelism:           concurrentUsers,
				burst:                 burst,
				rate:                  rate,
				constructor:           limiter.c,
				avgConcurrencyPerUser: 8,
				numServers:            2,
			})
		}
	}
}

type benchmarkDistributedConfig struct {
	name                  string
	parallelism           int
	avgConcurrencyPerUser int
	burst                 int
	rate                  float64
	constructor           func() Ratelimiter
	numServers            int64
}

func benchmarkDistributed(b *testing.B, c benchmarkDistributedConfig) bool {
	maxAllowed, maxDisallowed, maxLapsed := 0.0, 0.0, 0.0
	name := strings.ReplaceAll(c.name, " ", "_")
	runResult := b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(c.parallelism)
		allowed := atomic.Int64{}
		disallowed := atomic.Int64{}
		rls := make([]Ratelimiter, 0, c.numServers)
		for range c.numServers {
			rls = append(rls, c.constructor())
		}
		b.RunParallel(func(pb *testing.PB) {
			// To simulate different users hitting different rate limiters in a distributed system
			rStr := strconv.FormatInt(utils.RandInt(0, int64(c.parallelism/c.avgConcurrencyPerUser)), 10)
			rl := rls[utils.RandInt(0, c.numServers)]
			a := int64(0)
			d := int64(0)
			for pb.Next() {
				if ok, _ := rl.AllowN(rStr, 1, c.rate, c.burst); ok {
					a++
				} else {
					d++
				}
			}
			allowed.Add(int64(a))
			disallowed.Add(int64(d))
		})
		lapsed := b.Elapsed().Seconds()
		if lapsed > maxLapsed {
			maxLapsed = lapsed
			maxAllowed = float64(allowed.Load())
			maxDisallowed = float64(disallowed.Load())
		}
	})

	fmt.Printf("%s/custom_metrics\tallowed=%f\tdisallowed=%f\tlapsed=%f\tallowed(rps)=%f\ttotal(rps)=%f\n", b.Name()+"/"+name, maxAllowed, maxDisallowed, maxLapsed, maxAllowed/maxLapsed, (maxAllowed+maxDisallowed)/maxLapsed)
	return runResult
}
