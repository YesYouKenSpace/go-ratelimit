package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yesyoukenspace/go-ratelimit/internal/test_utils"
)

func TestDistributedAllow(t *testing.T) {
	tests := []testDistributedRatelimiterConfig{
		{
			reqPerSec:       4000,
			burst:           4000,
			runPattern:      []time.Duration{10 * time.Second, 2 * time.Second, 10 * time.Second},
			expectedAllowed: 88000,
			tolerance:       0.015,
			instances:       10,
		},
	}

	ratelimiters := []struct {
		name        string
		constructor func() Ratelimiter
	}{
		{
			name: "Redis",
			constructor: func() Ratelimiter {
				return NewGoRedis(newRDB(0))
			},
		},
		{
			name: "Redis with delay in sync",
			constructor: func() Ratelimiter {
				return NewRedisDelayedSync(context.Background(), RedisDelayedSyncOption{
					RedisClient:     newRDB(1),
					SyncInterval:    time.Second / 100,
					DisableAutoSync: false,
				})
			},
		},
	}
	for _, ratelimiter := range ratelimiters {
		for _, tt := range tests {
			testDistributedRatelimiter(t, ratelimiter.name, ratelimiter.constructor, tt)
		}
	}
}

type testDistributedRatelimiterConfig struct {
	reqPerSec       float64
	burst           int
	runPattern      []time.Duration
	expectedAllowed int
	tolerance       float64
	instances       int
}

func testDistributedRatelimiter(t *testing.T, name string, constructor func() Ratelimiter, tt testDistributedRatelimiterConfig) {
	t.Run(fmt.Sprintf("ratelimiter=%s;rps=%2f;burst=%d;instances=%d", name, tt.reqPerSec, tt.burst, tt.instances), func(t *testing.T) {
		t.Parallel()
		rStr := test_utils.RandString(4)
		totalAllowed := 0
		totalDenied := 0
		ratelimiters := make([]Ratelimiter, tt.instances)
		resultsChan := make(chan []int, tt.instances)
		for i := range tt.instances {
			ratelimiters[i] = constructor()
		}
		for _, ratelimiter := range ratelimiters {
			go func(ratelimiter Ratelimiter) {
				denied := 0
				allowed := 0
				for i, runFor := range tt.runPattern {
					if i%2 == 1 {
						time.Sleep(runFor)
						continue
					}

					ticker := time.NewTicker(time.Millisecond)
					timer := time.NewTimer(runFor)

				L:
					for {
						select {
						case <-ticker.C:
							if ok, _ := ratelimiter.AllowN(rStr, 1, tt.reqPerSec, tt.burst); ok {
								allowed++
							} else {
								denied++
							}
						case <-timer.C:
							ticker.Stop()
							break L
						}
					}
				}
				resultsChan <- []int{allowed, denied}
			}(ratelimiter)
		}

		for range tt.instances {
			results := <-resultsChan
			totalAllowed += results[0]
			totalDenied += results[1]
		}

		if !test_utils.IsCloseEnough(float64(tt.expectedAllowed), float64(totalAllowed), tt.tolerance) {
			t.Errorf("unexpected allowed: expected %d, got %d", tt.expectedAllowed, totalAllowed)
		}
		if totalDenied < 1 {
			t.Errorf("expected at least 1 denied request")
		}
		t.Logf("totalAllowed: %d, totalDenied: %d", totalAllowed, totalDenied)
	})
}
