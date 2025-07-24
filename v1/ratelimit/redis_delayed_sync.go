package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yesyoukenspace/go-ratelimit/limiter"
)

type RedisDelayedSyncCorruptedRemotePolicy string

const (
	// UPLOAD_LOCAL: Upload the local lastSynced value to redis
	// This is the default policy
	// This policy uploads the local lastSynced value to redis, and delays the delta push until the next sync.
	// If multiple servers are running, the largest `lastSynced` value will be the one that persists.
	// This policy goal is to preserve the deltas in the individual servers and effect the global rate limit with this policy,
	// with the assumption that the individual servers' sync are evenly spaced.
	RedisDelayedSyncCorruptedRemotePolicyUploadLocal RedisDelayedSyncCorruptedRemotePolicy = "UPLOAD_LOCAL"
	// RESET: Reset the key to 0
	// This policy effectively resets the key, delta push will be delayed until the next sync.
	// Upon the next sync, it would be as though it is the key's first sync.
	RedisDelayedSyncCorruptedRemotePolicyReset RedisDelayedSyncCorruptedRemotePolicy = "RESET"
)

type RedisDelayedSync struct {
	syncInterval          time.Duration
	ctx                   context.Context
	cancel                context.CancelFunc
	inner                 *SyncMapLoadThenLoadOrStore[*limiter.ResetBasedLimiter]
	redisClient           *redis.Client
	lastSyncedResetAt     sync.Map
	syncErrorHandler      func(error)
	keyExpiry             time.Duration
	corruptedRemotePolicy RedisDelayedSyncCorruptedRemotePolicy
}

type RedisDelayedSyncOption struct {
	// SyncInterval is the interval to sync the rate limit to the redis
	// Adjust this value to trade off between the performance and the accuracy of the rate limit
	SyncInterval          time.Duration
	RedisClient           *redis.Client
	SyncErrorHandler      func(error)
	KeyExpiry             time.Duration
	DisableAutoSync       bool
	CorruptedRemotePolicy RedisDelayedSyncCorruptedRemotePolicy
}

func NewRedisDelayedSync(ctx context.Context, opt RedisDelayedSyncOption) *RedisDelayedSync {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	corruptedRemotePolicy := RedisDelayedSyncCorruptedRemotePolicyUploadLocal
	if opt.CorruptedRemotePolicy != "" {
		corruptedRemotePolicy = opt.CorruptedRemotePolicy
	}

	rl := &RedisDelayedSync{
		ctx:                   ctx,
		cancel:                cancel,
		redisClient:           opt.RedisClient,
		syncInterval:          opt.SyncInterval,
		inner:                 NewSyncMapLoadThenLoadOrStore(limiter.NewResetbasedLimiter),
		lastSyncedResetAt:     sync.Map{},
		syncErrorHandler:      opt.SyncErrorHandler,
		keyExpiry:             opt.KeyExpiry,
		corruptedRemotePolicy: corruptedRemotePolicy,
	}
	if rl.syncErrorHandler == nil {
		rl.syncErrorHandler = func(err error) {
			fmt.Printf("error syncing: %v\n", err)
		}
	}
	if !opt.DisableAutoSync {
		rl.StartAutoSyncLoop()
	}
	return rl
}

func (r *RedisDelayedSync) StartAutoSyncLoop() {
	go func() {
		ticker := time.NewTicker(r.syncInterval)
		for {
			select {
			case <-r.ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				// Avoid overlapping calls to this function
				// We want syncAll to be called at most once at any given time thus we are not using a goroutine here
				if err := r.syncAll(); err != nil {
					if r.syncErrorHandler != nil {
						r.syncErrorHandler(err)
					}
				}
			}
		}
	}()
}

func (r *RedisDelayedSync) AllowN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	// Optimizations attempted here:
	// 1. Load Then LoadOrStore takes longer than just simply LoadOrStore, it may be due to us not using the returned value and there are compiler optimizations
	// 2. Using go routine with LoadOrStore ends up causing more allocations per operation and slowing down this operation
	_, _ = r.lastSyncedResetAt.LoadOrStore(key, 0)
	return r.inner.AllowN(key, cost, replenishPerSecond, burst)
}

func (r *RedisDelayedSync) ForceN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	// See AllowN for the optimizations attempted here
	_, _ = r.lastSyncedResetAt.LoadOrStore(key, 0)
	return r.inner.ForceN(key, cost, replenishPerSecond, burst)
}

// Note: This function is not thread safe
// Avoid overlapping calls to this function
func (r *RedisDelayedSync) syncAll() error {
	// -1 means no expiry
	expiry := int64(-1)
	// If the key expiry is set, use it to calculate the expiry time
	if r.keyExpiry > 0 {
		expiry = time.Now().Add(-r.keyExpiry).UnixNano()
	}
	// Consider using a different approach to prioritize syncing the keys that are used more frequently
	r.lastSyncedResetAt.Range(func(key, value any) bool {
		keyAsString := key.(string)
		err := r.sync(keyAsString, expiry)
		if err != nil {
			r.syncErrorHandler(err)
			return false
		}
		return true
	})
	return nil
}

func (r *RedisDelayedSync) executeCorruptedRemoteRecovery(key string, limiter *limiter.ResetBasedLimiter, delta int64, lastSynced int64) error {
	switch r.corruptedRemotePolicy {
	case RedisDelayedSyncCorruptedRemotePolicyUploadLocal:
		cmd := r.redisClient.Set(r.ctx, key, lastSynced, 0)
		if cmd.Err() != nil {
			return cmd.Err()
		}
	case RedisDelayedSyncCorruptedRemotePolicyReset:
		r.lastSyncedResetAt.Store(key, 0)
	default:
		return fmt.Errorf("invalid corrupted remote policy: %s", r.corruptedRemotePolicy)
	}

	// We need to add the delta back to the limiter and wait for the next sync.
	// We could have done SET lastSynced + delta but it would result in a race condition
	// where multiple servers could be setting the key at the same time and overwriting each other local delta.
	limiter.AddDeltaSinceLastPop(delta)
	return nil
}

// Note: This function is not thread safe
func (r *RedisDelayedSync) sync(key string, expiry int64) error {
	limiter := r.inner.GetLimiter(key)
	resetAt := limiter.GetResetAt()
	delta := limiter.PopResetAtDelta()
	lastSynced, hasLastSynced := r.lastSyncedResetAt.Load(key)
	hasSyncedBefore := hasLastSynced && lastSynced != 0
	// Case: Key's first sync
	if !hasSyncedBefore && resetAt > 0 {
		// we use `NX` to avoid overwriting the key if it is set by another server
		cmd := r.redisClient.SetNX(r.ctx, key, resetAt, 0)
		if cmd.Err() != nil {
			return cmd.Err()
		}
		// Case: The key is set by this server
		if cmd.Val() {
			r.lastSyncedResetAt.Store(key, resetAt)
			return nil
		}
		// if the key is not set by this server, we continue to the next step
	}

	// In this bloc, we are getting the remote value from redis and pushing the delta if any
	var remoteValue int64
	if delta > 0 {
		// Pushing delta to redis
		cmd := r.redisClient.IncrBy(r.ctx, key, delta)
		if cmd.Err() != nil {
			return cmd.Err()
		}
		remoteValue = cmd.Val()
	} else {
		cmd := r.redisClient.Get(r.ctx, key)
		if cmd.Err() == redis.Nil {
			if hasSyncedBefore {
				return r.executeCorruptedRemoteRecovery(key, limiter, delta, lastSynced.(int64))
			}
			return nil
		}
		if cmd.Err() != nil {
			return cmd.Err()
		}
		var err error
		remoteValue, err = strconv.ParseInt(cmd.Val(), 10, 64)
		if err != nil {
			return err
		}
	}
	if !hasSyncedBefore {
		// Case: The key is set by another server and the current server joins the cluster later
		// If the remote value is greater than the local resetAt due to clock drifts between servers,
		// this newly joined server will be penalized, we assume that the clock drift is not significant enough
		// Besides, the clock drift disadvantage is not permanent
		// After the first sync, the key will only sync the delta of the previously synced value and the next remote value
		if remoteValue > resetAt {
			limiter.IncrementResetAtBy(remoteValue - resetAt)
		}
		r.lastSyncedResetAt.Store(key, remoteValue)
		return nil
	}
	// Case: The remote value is corrupted, this could happen if redis server is restarted or if they were deleted
	// See `RedisDelayedSyncCorruptedRemotePolicy` for the policy to handle this case
	if remoteValue < lastSynced.(int64) {
		return r.executeCorruptedRemoteRecovery(key, limiter, delta, lastSynced.(int64))
	}
	// diff==0: if the key is not incremented by another server
	// diff>0: if the key is incremented by another server
	// this is the case where the clock drift could be an issue if the key is incremented by another server, the clock drift will affect calculation of the diff
	diff := remoteValue - lastSynced.(int64) - delta
	if resetAt < expiry && delta == 0 {
		r.lastSyncedResetAt.Delete(key)
		if diff == 0 {
			expireIn := max(r.keyExpiry, time.Until(time.Unix(0, remoteValue)))
			// this means that the redis key is in sync with the local resetAt value, meaning no other server has set the key in redis and we can expire the key in redis
			r.redisClient.ExpireNX(r.ctx, key, expireIn)
		}
		return nil
	}
	if diff > 0 {
		limiter.IncrementResetAtBy(diff)
	}
	r.lastSyncedResetAt.Store(key, remoteValue)
	return nil
}
