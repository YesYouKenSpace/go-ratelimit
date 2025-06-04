package limiter

import (
	"fmt"
	"math"
	"sync/atomic"

	"testing"
)

func BenchmarkLimiter(b *testing.B) {
	r := math.Pow10(6)
	burst := int(math.Pow10(1))
	limiters := []struct {
		name    string
		limiter Limiter
	}{
		{name: "Bucket", limiter: NewBucket()},
		{name: "DefaultLimiter", limiter: NewBuiltinLimiter(r, burst)},
		{name: "ResetBasedLimiter", limiter: NewResetbasedLimiter()},
	}

	for _, numberOfGoroutines := range []int{1, 8, 32} {
		for _, limiter := range limiters {
			limiterRunner(b, limiter.name, numberOfGoroutines, limiter.limiter, r, burst)
		}
	}
}

func limiterRunner(b *testing.B, name string, numberOfGoroutines int, limiter Limiter, rate float64, burst int) bool {
	maxLapsed, maxAllowed, maxDisallowed := 0.0, 0.0, 0.0
	runResult := b.Run(fmt.Sprintf("%s/goroutines=%d", name, numberOfGoroutines), func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(numberOfGoroutines)
		totalAllowed := atomic.Int64{}
		totalDisallowed := atomic.Int64{}
		b.RunParallel(func(pb *testing.PB) {
			allowed, disallowed := int64(0), int64(0)
			for pb.Next() {
				if limiter.AllowN(1, rate, burst) {
					allowed++
				} else {
					disallowed++
				}
			}
			totalAllowed.Add(allowed)
			totalDisallowed.Add(disallowed)
		})

		lapsed := b.Elapsed().Seconds()
		if lapsed > maxLapsed {
			maxLapsed = lapsed
			maxAllowed = float64(totalAllowed.Load())
			maxDisallowed = float64(totalDisallowed.Load())
		}
	})
	fmt.Printf("%s/custom_metrics\tallowed=%f\tdisallowed=%f\tlapsed=%f\tallowed(rps)=%f\ttotal(rps)=%f\n", b.Name()+"/"+name, maxAllowed, maxDisallowed, maxLapsed, maxAllowed/maxLapsed, (maxAllowed+maxDisallowed)/maxLapsed)
	return runResult
}
