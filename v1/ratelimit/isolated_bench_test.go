package ratelimit

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/yesyoukenspace/go-ratelimit/internal/test_utils"
)

func newRDB(dbIndexes ...int) *redis.Client {
	dbIndex := rand.Int() % 6
	if len(dbIndexes) > 0 {
		dbIndex = dbIndexes[0]
	}
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", MaxRetries: -1, PoolTimeout: 0, ContextTimeoutEnabled: true, DB: dbIndex})
	if err := client.Ping(context.Background()).Err(); err != nil {
		panic(err)
	}
	return client
}

func BenchmarkIsolated(b *testing.B) {
	rate := 3000.0
	burst := 3000
	redisClient := newRDB()

	ratelimiters := []struct {
		name    string
		limiter Ratelimiter
	}{
		{name: "Map + Mutex", limiter: NewMutex(NewDefaultLimiter)},
		{name: "Map + RWMutex", limiter: NewRWMutex(NewDefaultLimiter)},
		{name: "SyncMap + LoadOrStore", limiter: NewSyncMapLoadOrStore(NewDefaultLimiter)},
		{name: "SyncMap + Load > LoadOrStore", limiter: NewSyncMapLoadThenLoadOrStore(NewDefaultLimiter)},
		{name: "SyncMap + Load > Store", limiter: NewSyncMapLoadThenStore(NewDefaultLimiter)},
		{name: "Redis", limiter: NewGoRedis(redisClient)},
		{name: "Redis With Delay (2 syncs per second)", limiter: NewRedisDelayedSync(context.Background(), RedisDelayedSyncOption{
			RedisClient:  redisClient,
			SyncInterval: time.Second / 2,
		})},
		{name: "Redis With Delay (10 syncs per second)", limiter: NewRedisDelayedSync(context.Background(), RedisDelayedSyncOption{
			RedisClient:  redisClient,
			SyncInterval: time.Second / 10,
		})},
	}
	totalConcurrency := 32768
	if os.Getenv("TOTAL_CONCURRENCY") != "" {
		totalConcurrency, _ = strconv.Atoi(os.Getenv("TOTAL_CONCURRENCY"))
	}
	concurrencyPerUser := 8
	if os.Getenv("CONCURRENCY_PER_USER") != "" {
		concurrencyPerUser, _ = strconv.Atoi(os.Getenv("CONCURRENCY_PER_USER"))
	}
	fmt.Printf("%s/parameters\ttotalConcurrency: %d, concurrencyPerUser: %d, users: %d, rate: %f, burst: %d\n", b.Name(), totalConcurrency, concurrencyPerUser, totalConcurrency/concurrencyPerUser, rate, burst)
	for _, limiter := range ratelimiters {
		benchmarkIsolated(b, benchmarkIsolatedConfig{
			name:               limiter.name,
			totalConcurrency:   totalConcurrency,
			concurrencyPerUser: concurrencyPerUser,
			burst:              burst,
			rate:               rate,
			limiter:            limiter.limiter,
		})
	}
}

type benchmarkIsolatedConfig struct {
	name               string
	totalConcurrency   int
	concurrencyPerUser int
	limiter            Ratelimiter
	rate               float64
	burst              int
}

func benchmarkIsolated(b *testing.B, c benchmarkIsolatedConfig) bool {
	maxAllowed, maxDisallowed, maxLapsed := 0.0, 0.0, 0.0
	name := strings.ReplaceAll(c.name, " ", "_")
	runResult := b.Run(name, func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(c.totalConcurrency)
		allowed := atomic.Int64{}
		disallowed := atomic.Int64{}
		b.RunParallel(func(pb *testing.PB) {
			rStr := strconv.FormatInt(test_utils.RandInt(0, int64(c.totalConcurrency/c.concurrencyPerUser)), 10)
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
	fmt.Printf("%s/custom_metrics\tallowed=%f\tdisallowed=%f\tlapsed=%f\n", b.Name()+"/"+name, maxAllowed, maxDisallowed, maxLapsed)
	fmt.Printf("%s/custom_metrics\tallowed(rps)=%f\ttotal(rps)=%f\n", b.Name()+"/"+name, maxAllowed/maxLapsed, (maxAllowed+maxDisallowed)/maxLapsed)
	fmt.Printf("%s/custom_metrics\tallowed(rps/user)=%f\ttotal(rps/user)=%f\n", b.Name()+"/"+name, maxAllowed/maxLapsed/float64(c.totalConcurrency/c.concurrencyPerUser), (maxAllowed+maxDisallowed)/maxLapsed/float64(c.totalConcurrency/c.concurrencyPerUser))
	return runResult
}
