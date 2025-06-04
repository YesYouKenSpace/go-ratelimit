package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/yesyoukenspace/go-ratelimit/internal/utils"
)

func TestIsolatedAllow(t *testing.T) {
	tests := []testRatelimiterConfig{
		{
			reqPerSec:       100,
			burst:           10,
			runPattern:      []time.Duration{1 * time.Second, 1 * time.Second, 2 * time.Second},
			expectedAllowed: 320,
			tolerance:       0.01,
		},
		{
			reqPerSec:       10,
			burst:           100,
			runPattern:      []time.Duration{1500 * time.Millisecond, 11 * time.Second, 1500 * time.Millisecond},
			expectedAllowed: 230,
			tolerance:       0.01,
		},
		{
			reqPerSec:       100,
			burst:           100,
			runPattern:      []time.Duration{5 * time.Second, 2 * time.Second, 5 * time.Second},
			expectedAllowed: 1200,
			tolerance:       0.01,
		},
	}

	ratelimiters := []struct {
		name        string
		constructor func() Ratelimiter
	}{
		{
			name: "SyncMap + Load > LoadOrStore",
			constructor: func() Ratelimiter {
				return NewSyncMapLoadThenLoadOrStore(NewDefaultLimiter)
			},
		},
		{
			name: "SyncMap + Load > Store",
			constructor: func() Ratelimiter {
				return NewSyncMapLoadThenStore(NewDefaultLimiter)
			},
		},
		{
			name: "SyncMap + LoadOrStore",
			constructor: func() Ratelimiter {
				return NewSyncMapLoadOrStore(NewDefaultLimiter)
			},
		},
		{
			name: "Map + Mutex",
			constructor: func() Ratelimiter {
				return NewMutex(NewDefaultLimiter)
			},
		},
		{
			name: "Map + RWMutex",
			constructor: func() Ratelimiter {
				return NewRWMutex(NewDefaultLimiter)
			},
		},
		{
			name: "Redis",
			constructor: func() Ratelimiter {
				return NewGoRedis(newRDB())
			},
		},
		{
			name: "Redis with delay in sync",
			constructor: func() Ratelimiter {
				return NewRedisDelayedSync(context.Background(), RedisDelayedSyncOption{
					RedisClient:  newRDB(),
					SyncInterval: time.Second / 2,
				})
			},
		},
	}
	for _, ratelimiter := range ratelimiters {
		for _, tt := range tests {
			testRatelimiter(t, ratelimiter.name, ratelimiter.constructor, tt)
		}
	}
}

type testRatelimiterConfig struct {
	reqPerSec float64
	burst     int
	// runPattern is a list of durations to run the test for. Every odd index is a rest where no attempts on allow are made, every even/zero index is a run duration where attempts on allow are made.
	runPattern      []time.Duration
	expectedAllowed int
	tolerance       float64
}

func testRatelimiter(t *testing.T, name string, constructor func() Ratelimiter, tt testRatelimiterConfig) {
	t.Run(fmt.Sprintf("ratelimiter=%s;rps=%2f;burst=%d", name, tt.reqPerSec, tt.burst), func(t *testing.T) {
		rStr := utils.RandString(4)
		t.Parallel()
		allowed := 0
		denied := 0
		limiterGroup := constructor()
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
					if ok, _ := limiterGroup.AllowN(rStr, 1, tt.reqPerSec, tt.burst); ok {
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
		if !utils.IsCloseEnough(float64(tt.expectedAllowed), float64(allowed), tt.tolerance) {
			t.Errorf("expected %d, got %d", tt.expectedAllowed, allowed)
		}
		if denied < 1 {
			t.Errorf("expected >0 denied requests, got %d", denied)
		}
	})
}
