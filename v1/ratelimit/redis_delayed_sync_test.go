package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yesyoukenspace/go-ratelimit/internal/test_utils"
)

func TestRedisDelayedSync(t *testing.T) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ratelimiterAlpha := NewRedisDelayedSync(context.Background(), RedisDelayedSyncOption{
		RedisClient:           redisClient,
		DisableAutoSync:       true,
		CorruptedRemotePolicy: RedisDelayedSyncCorruptedRemotePolicyUploadLocal,
	})
	ratelimiterBeta := NewRedisDelayedSync(context.Background(), RedisDelayedSyncOption{
		RedisClient:           redisClient,
		DisableAutoSync:       true,
		CorruptedRemotePolicy: RedisDelayedSyncCorruptedRemotePolicyUploadLocal,
	})

	t.Run("GetResetAt should return 0 when the key is not used yet", func(t *testing.T) {
		randomString := test_utils.RandString(10)
		if ratelimiterBeta.inner.GetLimiter(randomString).GetResetAt() != 0 {
			t.Fatalf("reset at should be 0, but got %d", ratelimiterBeta.inner.GetLimiter(randomString).GetResetAt())
		}
	})

	t.Run("Synchronization tests", func(t *testing.T) {

		t.Run("diffs should be equal after a full cycle of sync if there are no actions in between", func(t *testing.T) {
			randomString := test_utils.RandString(10)
			_, _ = ratelimiterAlpha.ForceN(randomString, 1, 1, 1)
			_ = ratelimiterAlpha.sync(randomString, 0)
			originalResetAtOfAlpha := ratelimiterAlpha.inner.GetLimiter(randomString).GetResetAt()

			_ = ratelimiterBeta.sync(randomString, 0)
			_, _ = ratelimiterBeta.ForceN(randomString, 1, 1, 1)
			originalResetAtOfBeta := ratelimiterBeta.inner.GetLimiter(randomString).GetResetAt()

			_, _ = ratelimiterAlpha.ForceN(randomString, 1, 1, 1)
			_, _ = ratelimiterBeta.ForceN(randomString, 1, 1, 1)

			diffA := ratelimiterAlpha.inner.GetLimiter(randomString).GetResetAt() - originalResetAtOfAlpha
			diffB := ratelimiterBeta.inner.GetLimiter(randomString).GetResetAt() - originalResetAtOfBeta
			if diffA != 1*time.Second.Nanoseconds() {
				t.Logf("originalResetAtOfAlpha: %d", originalResetAtOfAlpha)
				t.Errorf("diff should be 1 second, but got %d nanoseconds", diffA)
			}
			if diffB != 1*time.Second.Nanoseconds() {
				t.Logf("originalResetAtOfBeta: %d", originalResetAtOfBeta)
				t.Errorf("diff should be 1 second, but got %d nanoseconds", diffB)
			}

			// full cycle of sync for both servers with no actions in between
			_ = ratelimiterAlpha.sync(randomString, 0)
			_ = ratelimiterBeta.sync(randomString, 0)
			_ = ratelimiterAlpha.sync(randomString, 0)
			_ = ratelimiterBeta.sync(randomString, 0)

			diffA = ratelimiterAlpha.inner.GetLimiter(randomString).GetResetAt() - originalResetAtOfAlpha
			diffB = ratelimiterBeta.inner.GetLimiter(randomString).GetResetAt() - originalResetAtOfBeta
			if diffA != 3*time.Second.Nanoseconds() {
				t.Logf("originalResetAtOfAlpha: %d", originalResetAtOfAlpha)
				t.Errorf("diff should be 3 second, but got %d nanoseconds", diffA)
			}
			if diffB != 2*time.Second.Nanoseconds() {
				t.Logf("originalResetAtOfBeta: %d", originalResetAtOfBeta)
				t.Errorf("diff should be 2 second, but got %d nanoseconds", diffB)
			}
		})

		t.Run("a server that joins the cluster later should not be able to allow more requests than the server that joined first", func(t *testing.T) {
			randomString := test_utils.RandString(10)
			if _, err := ratelimiterAlpha.ForceN(randomString, 600, 10, 10); err != nil {
				t.Fatalf("failed to force: %v", err)
			}
			if err := ratelimiterAlpha.sync(randomString, 0); err != nil {
				t.Fatalf("failed to sync: %v", err)
			}
			allowed, err := ratelimiterBeta.AllowN(randomString, 1, 10, 10)
			if err != nil {
				t.Fatalf("failed to allow: %v", err)
			}
			// Since the beta server has not synced yet, it should be allowed to allow 1 request
			if !allowed {
				t.Fatalf("should be allowed")
			}
			if err := ratelimiterBeta.sync(randomString, 0); err != nil {
				t.Fatalf("failed to sync: %v", err)
			}
			allowed, err = ratelimiterBeta.AllowN(randomString, 1, 10, 10)
			if err != nil {
				t.Fatalf("failed to allow: %v", err)
			}
			if allowed {
				t.Fatalf("should not be allowed")
			}
			allowed, err = ratelimiterAlpha.AllowN(randomString, 1, 10, 10)
			if err != nil {
				t.Fatalf("failed to allow: %v", err)
			}
			if allowed {
				t.Fatalf("should not be allowed")
			}
		})

		t.Run("RedisDelayedSyncCorruptedRemotePolicyUploadLocal", func(t *testing.T) {
			ratelimiterAlpha.corruptedRemotePolicy = RedisDelayedSyncCorruptedRemotePolicyUploadLocal
			ratelimiterBeta.corruptedRemotePolicy = RedisDelayedSyncCorruptedRemotePolicyUploadLocal

			randomString := test_utils.RandString(10)
			_, _ = ratelimiterAlpha.ForceN(randomString, 2, 1, 1000)
			_ = ratelimiterAlpha.sync(randomString, 0)
			_ = ratelimiterBeta.sync(randomString, 0)

			corrupt := func() {
				redisClient.Set(context.Background(), randomString, time.Now().Add(-1*time.Hour).Unix(), 0)
			}

			testCases := []struct {
				scenario     func()
				expectedDiff time.Duration
			}{
				{
					scenario: func() {
						_, _ = ratelimiterAlpha.ForceN(randomString, 3, 1, 1000)
						_, _ = ratelimiterBeta.ForceN(randomString, 5, 1, 1000)
						corrupt()
					},
					expectedDiff: 8 * time.Second,
				},
				{
					scenario: func() {
						_, _ = ratelimiterAlpha.ForceN(randomString, 3, 1, 1000)
						_ = ratelimiterAlpha.sync(randomString, 0)
						corrupt()
						_, _ = ratelimiterBeta.ForceN(randomString, 5, 1, 1000)
						_ = ratelimiterBeta.sync(randomString, 0)
					},
					expectedDiff: 8 * time.Second,
				},
				{
					scenario: func() {
						_, _ = ratelimiterAlpha.ForceN(randomString, 3, 1, 1000)
						_ = ratelimiterAlpha.sync(randomString, 0)
						corrupt()
						_, _ = ratelimiterAlpha.ForceN(randomString, 5, 1, 1000)
						_, _ = ratelimiterBeta.ForceN(randomString, 7, 1, 1000)
						_ = ratelimiterBeta.sync(randomString, 0)
						corrupt()
					},
					expectedDiff: 15 * time.Second,
				},
				{
					// This test case shows that if a server manages to sync twice before the other server even syncs once after corruption
					// We lost delta of the sync that happened before the corruption
					scenario: func() {
						_, _ = ratelimiterAlpha.ForceN(randomString, 3, 1, 1000)
						_ = ratelimiterAlpha.sync(randomString, 0)
						corrupt()
						_, _ = ratelimiterAlpha.ForceN(randomString, 5, 1, 1000)
						_, _ = ratelimiterBeta.ForceN(randomString, 7, 1, 1000)
						_ = ratelimiterBeta.sync(randomString, 0)
						_ = ratelimiterBeta.sync(randomString, 0)
					},

					expectedDiff: 12 * time.Second,
				},
			}
			for i, testCase := range testCases {
				t.Run(fmt.Sprintf("test case %d", i), func(t *testing.T) {
					originalResetAtOfBeta := ratelimiterBeta.inner.GetLimiter(randomString).GetResetAt()
					originalResetAtOfAlpha := ratelimiterAlpha.inner.GetLimiter(randomString).GetResetAt()

					testCase.scenario()
					// full cycle of sync for both servers with no actions in between
					_ = ratelimiterAlpha.sync(randomString, 0)
					_ = ratelimiterBeta.sync(randomString, 0)
					_ = ratelimiterAlpha.sync(randomString, 0)
					_ = ratelimiterBeta.sync(randomString, 0)

					diffA := ratelimiterAlpha.inner.GetLimiter(randomString).GetResetAt() - originalResetAtOfAlpha
					diffB := ratelimiterBeta.inner.GetLimiter(randomString).GetResetAt() - originalResetAtOfBeta
					if diffA != diffB {
						t.Fatalf("diff should be equal, but got %d and %d", diffA, diffB)
					}
					if diffA != testCase.expectedDiff.Nanoseconds() {
						t.Fatalf("diff should be %d, but got %d", testCase.expectedDiff.Nanoseconds(), diffA)
					}
				})
			}

		})
	})

	t.Run("RedisDelayedSyncCorruptedRemotePolicyReset", func(t *testing.T) {
		ratelimiterAlpha.corruptedRemotePolicy = RedisDelayedSyncCorruptedRemotePolicyReset
		ratelimiterBeta.corruptedRemotePolicy = RedisDelayedSyncCorruptedRemotePolicyReset

		randomString := test_utils.RandString(10)
		_, _ = ratelimiterAlpha.ForceN(randomString, 2, 1, 1000)
		_ = ratelimiterAlpha.sync(randomString, 0)
		_ = ratelimiterBeta.sync(randomString, 0)
		// Corrupt the remote value
		redisClient.Set(context.Background(), randomString, time.Now().Add(-1*time.Hour).Unix(), 0)
		_ = ratelimiterAlpha.sync(randomString, 0)
		_ = ratelimiterBeta.sync(randomString, 0)

		lastSyncedResetAtOfAlpha, _ := ratelimiterAlpha.lastSyncedResetAt.Load(randomString)
		lastSyncedResetAtOfBeta, _ := ratelimiterBeta.lastSyncedResetAt.Load(randomString)
		if lastSyncedResetAtOfAlpha != 0 {
			t.Fatalf("last synced reset at should be 0, but got %d", lastSyncedResetAtOfAlpha)
		}
		if lastSyncedResetAtOfBeta != 0 {
			t.Fatalf("last synced reset at should be 0, but got %d", lastSyncedResetAtOfBeta)
		}
	})
}
