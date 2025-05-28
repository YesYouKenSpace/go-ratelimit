package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yesyoukenspace/go-ratelimit/limiter"
)

type RedisDelayedSync struct {
	syncInterval     time.Duration
	ctx              context.Context
	cancel           context.CancelFunc
	inner            *SyncMapLoadThenStore[*limiter.ResetBasedLimiter]
	redisClient      *redis.Client
	indexToSync      atomic.Int32
	toSync           []sync.Map
	lastSynced       map[string]int64
	syncErrorHandler func(error)
	keyExpiry        time.Duration
}

type RedisDelayedSyncOption struct {
	// SyncInterval is the interval to sync the rate limit to the redis
	// Adjust this value to trade off between the performance and the accuracy of the rate limit
	SyncInterval         time.Duration
	ReplenishedPerSecond float64
	Burst                int
	RedisClient          *redis.Client
	SyncErrorHandler     func(error)
	KeyExpiry            time.Duration
}

func NewRedisDelayedSync(ctx context.Context, opt RedisDelayedSyncOption) *RedisDelayedSync {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	rl := &RedisDelayedSync{
		ctx:              ctx,
		cancel:           cancel,
		redisClient:      opt.RedisClient,
		syncInterval:     opt.SyncInterval,
		inner:            NewSyncMapLoadThenStore(limiter.NewResetbasedLimiter, opt.ReplenishedPerSecond, opt.Burst),
		toSync:           make([]sync.Map, 1),
		lastSynced:       map[string]int64{},
		syncErrorHandler: opt.SyncErrorHandler,
	}
	if rl.syncErrorHandler == nil {
		rl.syncErrorHandler = func(err error) {
			fmt.Printf("error syncing: %v\n", err)
		}
	}
	go func() {
		ticker := time.NewTicker(opt.SyncInterval)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				indexToSync := rl.indexToSync.Load()
				rl.indexToSync.Store((indexToSync + 1) % int32(len(rl.toSync)))
				if err := rl.syncAll(indexToSync); err != nil {
					if rl.syncErrorHandler != nil {
						rl.syncErrorHandler(err)
					}
				}
			}
		}
	}()
	return rl
}

func (r *RedisDelayedSync) Allow(key string) (bool, error) {
	return r.AllowN(key, 1)
}

func (r *RedisDelayedSync) AllowN(key string, cost int) (bool, error) {
	r.toSync[r.indexToSync.Load()].LoadOrStore(key, struct{}{})
	return r.inner.AllowN(key, cost)
}

// Note: This function is not thread safe
func (r *RedisDelayedSync) syncAll(index int32) error {
	expiry := time.Now().Add(-r.keyExpiry).UnixNano()
	// Consider using a different approach to prioritize syncing the keys that are used more frequently
	r.toSync[index].Range(func(key, value any) bool {
		if err := r.sync(key.(string), expiry); err != nil {
			return false
		}
		return true
	})
	// BUG: we used to clear the toSync[index] after syncing, but this will cause issues on keys that are used infrequently and the sync interval is large enough, the key could use up the burst limit before the key is synced, then skip a sync interval, and then use up the burst limit again on multiple instances
	// Assumption: clear finishes before r.currentIndex rotates back to the index that is being cleared
	// If the assumption is not true, the key will be synced again in the next sync interval if the key is used again
	// We can avoid this, by:
	// A: if the syncInterval is large enough to ensure that the clear finishes before the next sync
	// B: if the array of sync.Map is large enough to ensure that the clear finishes before a full rotation is made
	// go r.toSync[index].Clear()
	return nil
}

// Note: This function is not thread safe
func (r *RedisDelayedSync) sync(key string, expiry int64) error {
	limiter := r.inner.GetLimiter(key)
	resetAt := limiter.GetResetAt()

	delta := limiter.PopResetAtDelta()
	// If the key is not synced yet, this could be the first time the key is used across the entire distributed environment
	// We need to set the key in the redis
	if r.lastSynced[key] == 0 {
		// TODO: fix arbitrary 48 hours expiry
		cmd := r.redisClient.SetNX(r.ctx, key, resetAt, time.Hour*48)
		if cmd.Err() != nil {
			return cmd.Err()
		}
		// We assume that this server is the first server to set the key in the redis
		// There is no need to check the result of the SetNX command, as it does not matter if the key is set by another server or not,
		// as long as the key is set, the next sync will be able to calculate the correct delta, and the key will be in sync
		// there is also no need to set the lastSynced value to the value in the redis, even if the key is set by another server,
		// because we will later calculate the difference between the value in redis with the local resetAt value
		r.lastSynced[key] = int64(resetAt)
	}
	cmd := r.redisClient.IncrBy(r.ctx, key, int64(delta))
	if cmd.Err() != nil {
		return cmd.Err()
	}
	// if lastSynced value was previously set to the local resetAt value
	// diff==0: if the key is not set by another server
	// diff>0: if the key is set by another server and the current server joined the cluster later -
	// this is the case where the clock drift could be an issue if the key is set by another server, the clock drift will affect calculation of the diff
	diff := cmd.Val() - r.lastSynced[key] - delta
	if resetAt < expiry {
		return nil
	}
	if diff > 0 {
		limiter.IncrementResetAtBy(diff)
	}
	r.lastSynced[key] = cmd.Val()
	return nil
}
