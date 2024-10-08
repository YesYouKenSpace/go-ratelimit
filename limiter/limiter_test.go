package limiter

import (
	"fmt"
	"testing"
	"time"

	"github.com/yesyoukenspace/go-ratelimit/internal/utils"
	"golang.org/x/time/rate"
)

var limiters = []struct {
	name        string
	constructor func(float64, int) Limiter
}{
	{
		name: "golang.org/x/time/rate",
		constructor: func(limit float64, burst int) Limiter {
			return NewSimpleLimiterAdapter(rate.NewLimiter(rate.Limit(limit), burst))
		},
	},
	{
		name: "Bucket",
		constructor: func(limit float64, burst int) Limiter {
			return NewBucket(limit, burst)
		},
	},
}

func TestLimiterAllow(t *testing.T) {
	tests := []struct {
		name            string
		reqPerSec       float64
		burst           int
		runFor          time.Duration
		expectedAllowed int
		tolerance       float64
	}{
		{
			reqPerSec:       10,
			burst:           1,
			runFor:          3 * time.Second,
			expectedAllowed: 31,
			tolerance:       1,
		},
		{
			reqPerSec:       1,
			burst:           100,
			runFor:          3 * time.Second,
			expectedAllowed: 103,
			tolerance:       1,
		},
	}

	for _, limiter := range limiters {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("limiter=%s;rps=%2f;burst=%d", limiter.name, tt.reqPerSec, tt.burst), func(t *testing.T) {
				allowed := 0
				denied := 0
				ticker := time.NewTicker(time.Millisecond / 2)
				timer := time.NewTimer(tt.runFor)
				limiter := limiter.constructor(tt.reqPerSec, tt.burst)
			L:
				for {
					select {
					case <-ticker.C:
						if ok, _ := limiter.Allow(); ok {
							allowed++
						} else {
							denied++
						}
					case <-timer.C:
						ticker.Stop()
						break L
					}
				}
				if !utils.IsCloseEnough(float64(tt.expectedAllowed), float64(allowed), tt.tolerance) {
					t.Errorf("expected %d, got %d", tt.expectedAllowed, allowed)
				}
				if allowed+denied < int(tt.reqPerSec)*int(tt.runFor.Seconds()) {
					t.Errorf("expected >%d runs, got %d", int(tt.reqPerSec)*int(tt.runFor.Seconds()), allowed+denied)
				}
			})
		}
	}
}
