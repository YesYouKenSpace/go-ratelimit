package ratelimit

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/yesyoukenspace/go-ratelimit/internal/utils"
)

func newRDB(dbIndexes ...int) *redis.Client {
	dbIndex := rand.Int() % 6
	if len(dbIndexes) > 0 {
		dbIndex = dbIndexes[0]
	}
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", MaxRetries: -1, DialTimeout: 20 * time.Millisecond, PoolTimeout: 0, ContextTimeoutEnabled: true, DB: dbIndex})
	if err := client.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}
	return client
}

func BenchmarkIsolated(b *testing.B) {
	rate := math.Pow10(6)
	burst := int(math.Pow10(6))

	ratelimiters := []struct {
		name    string
		limiter Ratelimiter
	}{
		{name: "SyncMap + LoadOrStore", limiter: NewSyncMapLoadOrStore(NewDefaultLimiter)},
		{name: "SyncMap + Load > LoadOrStore", limiter: NewSyncMapLoadThenLoadOrStore(NewDefaultLimiter)},
		{name: "SyncMap + Load > Store", limiter: NewSyncMapLoadThenStore(NewDefaultLimiter)},
		{name: "Map + Mutex", limiter: NewMutex(NewDefaultLimiter)},
		{name: "Map + RWMutex", limiter: NewRWMutex(NewDefaultLimiter)},
		{name: "Redis", limiter: NewGoRedis(newRDB())},
		{name: "Redis With Delay", limiter: NewRedisDelayedSync(context.Background(), RedisDelayedSyncOption{
			RedisClient:  newRDB(),
			SyncInterval: time.Second / 2,
		})},
	}
	concurrentUsers := 32768
	if os.Getenv("CONCURRENT_USERS") != "" {
		concurrentUsers, _ = strconv.Atoi(os.Getenv("CONCURRENT_USERS"))
	}
	for _, limiter := range ratelimiters {
		benchmarkIsolated(b, benchmarkIsolatedConfig{
			name:            limiter.name,
			concurrentUsers: concurrentUsers,
			burst:           burst,
			rate:            rate,
			limiter:         limiter.limiter,
		})
	}
}

type benchmarkIsolatedConfig struct {
	name            string
	concurrentUsers int
	limiter         Ratelimiter
	rate            float64
	burst           int
}

func benchmarkIsolated(b *testing.B, c benchmarkIsolatedConfig) bool {
	maxAllowed, maxDisallowed, maxLapsed := 0.0, 0.0, 0.0
	name := strings.ReplaceAll(c.name, " ", "_")
	runResult := b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(c.concurrentUsers)
		allowed := atomic.Int64{}
		disallowed := atomic.Int64{}
		b.RunParallel(func(pb *testing.PB) {
			rStr := utils.RandString(4)
			a := int64(0)
			d := int64(0)
			for pb.Next() {
				if ok, _ := c.limiter.AllowN(rStr, 1, c.rate, c.burst); ok {
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
