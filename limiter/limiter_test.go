package limiter

import (
	"fmt"
	"testing"
	"time"

	"github.com/yesyoukenspace/go-ratelimit/internal/utils"
)

var limiters = []struct {
	name         string
	newLimiterFn func(replenishPerSecond float64, burst int) Limiter
}{
	{
		name: "golang.org/x/time/rate",
		newLimiterFn: func(replenishPerSecond float64, burst int) Limiter {
			return NewBuiltinLimiter(replenishPerSecond, burst)
		},
	},
	{
		name: "Bucket",
		newLimiterFn: func(replenishPerSecond float64, burst int) Limiter {
			return NewBucket()
		},
	},
	{
		name: "ResetbasedLimiter",
		newLimiterFn: func(replenishPerSecond float64, burst int) Limiter {
			return NewResetbasedLimiter()
		},
	},
}

type testLimiterConfig struct {
	replenishPerSecond float64
	burst              int
	runPattern         []time.Duration
	expectedAllowed    int
	tolerance          float64
}

func TestLimiterAllow(t *testing.T) {
	tests := []testLimiterConfig{
		{
			replenishPerSecond: 100,
			burst:              100,
			// 2 seconds of run
			runPattern:      []time.Duration{2 * time.Second},
			expectedAllowed: 300,
			tolerance:       0.001,
		},
		{
			replenishPerSecond: 100,
			burst:              10,
			runPattern:         []time.Duration{1 * time.Second, 1 * time.Second, 2 * time.Second},
			expectedAllowed:    320,
			tolerance:          0.01,
		},
		{
			replenishPerSecond: 10,
			burst:              100,
			runPattern:         []time.Duration{1 * time.Second, 11 * time.Second, 2 * time.Second},
			expectedAllowed:    230,
			tolerance:          0.01,
		},
		{
			replenishPerSecond: 500,
			burst:              1000,
			runPattern:         []time.Duration{2 * time.Second, 1 * time.Second, 2 * time.Second},
			expectedAllowed:    3500,
			tolerance:          0.01,
		},
	}

	for _, limiterConfig := range limiters {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("limiter=%s;rps=%2f;burst=%d", limiterConfig.name, tt.replenishPerSecond, tt.burst), func(t *testing.T) {
				t.Parallel()
				allowed := 0
				denied := 0
				limiter := limiterConfig.newLimiterFn(tt.replenishPerSecond, tt.burst)

				for i, runFor := range tt.runPattern {
					if i%2 == 1 {
						time.Sleep(runFor)
						continue
					}
					ticker := time.NewTicker(time.Millisecond / 2)
					timer := time.NewTimer(runFor)
				L:
					for {
						select {
						case <-ticker.C:
							if limiter.AllowN(1, tt.replenishPerSecond, tt.burst) {
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
					t.Errorf("expected >%d denials, got %d", 1, denied)
				}
			})
		}
	}
}
