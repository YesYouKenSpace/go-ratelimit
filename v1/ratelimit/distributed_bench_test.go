package ratelimit

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"testing"

	"github.com/yesyoukenspace/go-ratelimit/internal/utils"
)

func BenchmarkDistributed(b *testing.B) {
	rate := 3000.0
	burst := 3000
	ratelimiters := []struct {
		name string
		c    func() Ratelimiter
	}{
		{name: "Redis", c: func() Ratelimiter { return NewGoRedis(newRDB(2)) }},
		{name: "Redis with delay in sync (2 syncs per second)", c: func() Ratelimiter {
			return NewRedisDelayedSync(context.Background(), RedisDelayedSyncOption{
				RedisClient:  newRDB(3),
				SyncInterval: time.Second / 2,
			})
		},
		},
		{name: "Redis with delay in sync (10 syncs per second)", c: func() Ratelimiter {
			return NewRedisDelayedSync(context.Background(), RedisDelayedSyncOption{
				RedisClient:  newRDB(4),
				SyncInterval: time.Second / 10,
			})
		},
		},
	}
	totalConcurrency := 32768
	if os.Getenv("TOTAL_CONCURRENCY") != "" {
		totalConcurrency, _ = strconv.Atoi(os.Getenv("TOTAL_CONCURRENCY"))
	}
	numServers := 2
	if os.Getenv("NUM_SERVERS") != "" {
		numServers, _ = strconv.Atoi(os.Getenv("NUM_SERVERS"))
	}
	concurrencyPerUser := 8
	if os.Getenv("CONCURRENCY_PER_USER") != "" {
		concurrencyPerUser, _ = strconv.Atoi(os.Getenv("CONCURRENCY_PER_USER"))
	}

	fmt.Printf("%s/parameters\ttotalConcurrency: %d, concurrencyPerUser: %d, users: %d, numServers: %d, rate: %f, burst: %d\n", b.Name(), totalConcurrency, concurrencyPerUser, totalConcurrency/concurrencyPerUser, numServers, rate, burst)
	for _, limiter := range ratelimiters {
		benchmarkDistributed(b, benchmarkDistributedConfig{
			name:                  limiter.name,
			totalConcurrency:      totalConcurrency,
			burst:                 burst,
			rate:                  rate,
			constructor:           limiter.c,
			avgConcurrencyPerUser: concurrencyPerUser,
			numServers:            numServers,
		})
	}
}

type benchmarkDistributedConfig struct {
	name                  string
	totalConcurrency      int
	avgConcurrencyPerUser int
	burst                 int
	rate                  float64
	constructor           func() Ratelimiter
	numServers            int
}

func benchmarkDistributed(b *testing.B, c benchmarkDistributedConfig) bool {
	maxAllowed, maxDisallowed, maxLapsed := 0.0, 0.0, 0.0
	name := strings.ReplaceAll(c.name, " ", "_")
	runResult := b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(c.totalConcurrency)
		allowed := atomic.Int64{}
		disallowed := atomic.Int64{}
		rls := make([]Ratelimiter, 0, c.numServers)
		for range c.numServers {
			rls = append(rls, c.constructor())
		}
		b.RunParallel(func(pb *testing.PB) {
			// To simulate different users hitting different rate limiters in a distributed system
			rStr := strconv.FormatInt(utils.RandInt(0, int64(c.totalConcurrency/c.avgConcurrencyPerUser)), 10)
			rl := rls[utils.RandInt(0, int64(c.numServers))]
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
	fmt.Printf("%s/custom_metrics\tallowed=%f\tdisallowed=%f\tlapsed=%f\n", b.Name()+"/"+name, maxAllowed, maxDisallowed, maxLapsed)
	fmt.Printf("%s/custom_metrics\tallowed(rps)=%f\ttotal(rps)=%f\n", b.Name()+"/"+name, maxAllowed/maxLapsed, (maxAllowed+maxDisallowed)/maxLapsed)
	fmt.Printf("%s/custom_metrics\tallowed(rps/user)=%f\ttotal(rps/user)=%f\n", b.Name()+"/"+name, maxAllowed/maxLapsed/float64(c.totalConcurrency/c.avgConcurrencyPerUser), (maxAllowed+maxDisallowed)/maxLapsed/float64(c.totalConcurrency/c.avgConcurrencyPerUser))
	return runResult
}
