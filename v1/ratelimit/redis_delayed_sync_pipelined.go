package ratelimit

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yesyoukenspace/go-ratelimit/limiter"
)

const DefaultBatchSize = 300

//go:embed sync.lua
var syncScript string

type status string

const (
	OK              status = "ok"
	AdjustLocal     status = "adjust_local"
	CorruptedRemote status = "corrupted_remote"
	Expired         status = "expired"
)

type RedisDelayedSyncPipelined struct {
	syncInterval          time.Duration
	ctx                   context.Context
	cancel                context.CancelFunc
	inner                 *SyncMapLoadThenLoadOrStore[*limiter.ResetBasedLimiter]
	redisClient           *redis.Client
	redisPrefix           string
	lastSyncedResetAt     sync.Map
	syncErrorHandler      func(error)
	keyExpiry             time.Duration
	corruptedRemotePolicy RedisDelayedSyncCorruptedRemotePolicy
	batchSize             int

	observeSyncDuration  limiter.MetricUpdateFunc
	observeBatchDuration limiter.MetricUpdateFunc
	observeSyncedCount   limiter.MetricUpdateFunc
	observeSyncError     limiter.MetricUpdateFunc

	syncScriptSha atomic.Value
}

type RedisDelayedSyncPipelinedOption struct {
	// SyncInterval is the interval to sync the rate limit to the redis
	// Adjust this value to trade off between the performance and the accuracy of the rate limit
	SyncInterval time.Duration
	RedisClient  *redis.Client
	// RedisPrefix prefix to add to Redis key. Default is "yyks:gort::".
	RedisPrefix           string
	SyncErrorHandler      func(error)
	KeyExpiry             time.Duration
	DisableAutoSync       bool
	CorruptedRemotePolicy RedisDelayedSyncCorruptedRemotePolicy
	batchSize             int
	// ObserveSyncDuration is called with the duration taken to complete a syncAll operation
	ObserveSyncDuration limiter.MetricUpdateFunc
	// ObserveBatchDuration is called with the duration taken to complete a batch of commands in a syncAll operation
	ObserveBatchDuration limiter.MetricUpdateFunc
	// ObserveSyncedCount is called with the number of keys synced in a batch
	ObserveSyncedCount limiter.MetricUpdateFunc
	// ObserveSyncError is called with 1 for each sync error encountered
	ObserveSyncError limiter.MetricUpdateFunc
}

func MustNewRedisDelayedSyncPipelined(ctx context.Context, opt RedisDelayedSyncPipelinedOption) *RedisDelayedSyncPipelined {
	rdsp, err := NewRedisDelayedSyncPipelined(ctx, opt)
	if err != nil {
		panic(err)
	}

	return rdsp
}

func NewRedisDelayedSyncPipelined(ctx context.Context, opt RedisDelayedSyncPipelinedOption) (*RedisDelayedSyncPipelined, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	corruptedRemotePolicy := RedisDelayedSyncCorruptedRemotePolicyUploadLocal
	if opt.CorruptedRemotePolicy != "" {
		corruptedRemotePolicy = opt.CorruptedRemotePolicy
	}

	batchSize := opt.batchSize
	if batchSize == 0 {
		batchSize = DefaultBatchSize
	}

	prefix := "yyks:gort::"
	if len(opt.RedisPrefix) > 0 {
		prefix = opt.RedisPrefix
	}

	rl := &RedisDelayedSyncPipelined{
		ctx:                   ctx,
		cancel:                cancel,
		redisClient:           opt.RedisClient,
		redisPrefix:           prefix,
		syncInterval:          opt.SyncInterval,
		inner:                 NewSyncMapLoadThenLoadOrStore(limiter.NewResetbasedLimiter),
		lastSyncedResetAt:     sync.Map{},
		syncErrorHandler:      opt.SyncErrorHandler,
		keyExpiry:             opt.KeyExpiry,
		corruptedRemotePolicy: corruptedRemotePolicy,
		batchSize:             batchSize,

		observeSyncDuration:  opt.ObserveSyncDuration,
		observeBatchDuration: opt.ObserveBatchDuration,
		observeSyncedCount:   opt.ObserveSyncedCount,
		observeSyncError:     opt.ObserveSyncError,
	}
	if rl.syncErrorHandler == nil {
		rl.syncErrorHandler = func(err error) {
			_, _ = fmt.Fprintf(os.Stderr, "error syncing: %v\n", err)
		}
	}

	if err := rl.loadSyncScript(); err != nil {
		return nil, err
	}

	if !opt.DisableAutoSync {
		rl.StartAutoSyncLoop(ctx)
	}

	return rl, nil
}

func (r *RedisDelayedSyncPipelined) StartAutoSyncLoop(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(r.syncInterval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				// Avoid overlapping calls to this function
				// We want syncAll to be called at most once at any given time thus we are not using a goroutine here
				if err := r.syncAll(); err != nil {
					if r.syncErrorHandler != nil {
						r.syncErrorHandler(err)
						if r.observeSyncError != nil {
							r.observeSyncError(1)
						}
					}
				}
			}
		}
	}()
}

func (r *RedisDelayedSyncPipelined) AllowN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	// Optimizations attempted here:
	// 1. Load Then LoadOrStore takes longer than just simply LoadOrStore, it may be due to us not using the returned value and there are compiler optimizations
	// 2. Using go routine with LoadOrStore ends up causing more allocations per operation and slowing down this operation
	_, _ = r.lastSyncedResetAt.LoadOrStore(key, 0)
	return r.inner.AllowN(key, cost, replenishPerSecond, burst)
}

func (r *RedisDelayedSyncPipelined) ForceN(key string, cost int, replenishPerSecond float64, burst int) (bool, error) {
	// See AllowN for the optimizations attempted here
	_, _ = r.lastSyncedResetAt.LoadOrStore(key, 0)
	return r.inner.ForceN(key, cost, replenishPerSecond, burst)
}

type syncArgs struct {
	key   string
	cmd   *redis.Cmd
	delta int64
	lmt   *limiter.ResetBasedLimiter
}

// Note: This function is not thread safe
// Avoid overlapping calls to this function
func (r *RedisDelayedSyncPipelined) syncAll() (err error) {
	if r.observeSyncDuration != nil {
		defer func(start time.Time) {
			r.observeSyncDuration(time.Since(start).Seconds())
		}(time.Now())
	}
	var (
		commands  []syncArgs
		mustRetry bool // in case of missing script in Redis server
	)

	executePipeline := func(pipeline redis.Pipeliner) bool {
		if r.observeBatchDuration != nil {
			defer func(start time.Time) {
				r.observeBatchDuration(time.Since(start).Seconds())
			}(time.Now())
		}
		if r.observeSyncedCount != nil {
			defer r.observeSyncedCount(float64(pipeline.Len()))
		}
		_, err = pipeline.Exec(r.ctx)
		if err != nil {
			return false
		}

		for _, cmdArgs := range commands {
			cmdRes, err := cmdArgs.cmd.Result()
			if err != nil {
				if redis.HasErrorPrefix(err, "NOSCRIPT") {
					r.syncErrorHandler(err)
					if err = r.loadSyncScript(); err != nil {
						return false
					}

					mustRetry = true
					return false
				}

				r.syncErrorHandler(err)
				continue
			}

			if err := r.processSyncRes(cmdArgs, cmdRes); err != nil {
				r.syncErrorHandler(err)
			}
		}

		return true
	}

	// Consider using a different approach to prioritize syncing the keys that are used more frequently
	pipeliner := r.redisClient.Pipeline()

	r.lastSyncedResetAt.Range(func(key, lastSynced any) bool {
		commands = append(commands, r.pipelineSyncCmd(pipeliner, key, lastSynced))

		if len(commands) == r.batchSize {
			defer func() {
				// redis advises to create a new pipeline for each batch
				pipeliner = r.redisClient.Pipeline()
				commands = commands[:0]
			}()
			return executePipeline(pipeliner)
		}

		return true
	})

	if mustRetry {
		return r.syncAll()
	}

	if err == nil && len(commands) > 0 {
		_ = executePipeline(pipeliner)
	}

	return err
}

func (r *RedisDelayedSyncPipelined) executeCorruptedRemoteRecovery(key string, limiter *limiter.ResetBasedLimiter, delta int64, lastSynced int64) error {
	switch r.corruptedRemotePolicy {
	case RedisDelayedSyncCorruptedRemotePolicyUploadLocal:
		cmd := r.redisClient.Set(r.ctx, r.prefixKey(key), lastSynced, time.Hour)
		if cmd.Err() != nil {
			return cmd.Err()
		}
	case RedisDelayedSyncCorruptedRemotePolicyReset:
		r.lastSyncedResetAt.Store(key, 0)
	default:
		return fmt.Errorf("invalid corrupted remote policy: %s", r.corruptedRemotePolicy)
	}

	// We need to add the delta back to the limiter and wait for the next addSyncCommand.
	// We could have done SET lastSynced + delta but it would result in a race condition
	// where multiple servers could be setting the key at the same time and overwriting each other local delta.
	limiter.AddDeltaSinceLastPop(delta)
	return nil
}

// GetResetAt is a helper that returns the current resetAt value for a given key.
// Useful for asserting limiter state in tests.
func (r *RedisDelayedSyncPipelined) GetResetAt(key string) int64 {
	return r.inner.GetLimiter(key).GetResetAt()
}

func (r *RedisDelayedSyncPipelined) loadSyncScript() error {
	uploadCmd := r.redisClient.ScriptLoad(r.ctx, syncScript)
	sha, err := uploadCmd.Result()
	if err != nil {
		return fmt.Errorf("failed to upload sync script to Redis Via SCRIPT LOAD: %v", err)
	}

	r.syncScriptSha.Store(sha)
	return nil
}

func (r *RedisDelayedSyncPipelined) pipelineSyncCmd(pipeliner redis.Pipeliner, key, lastSynced any) syncArgs {
	keyAsString := key.(string)
	lmt := r.inner.GetLimiter(keyAsString)
	resetAt := lmt.GetResetAt()
	delta := lmt.PopResetAtDelta()

	cmd := pipeliner.EvalSha(r.ctx, r.syncScriptSha.Load().(string), []string{keyAsString},
		int64(r.keyExpiry),
		resetAt,
		delta,
		lastSynced,
	)

	return syncArgs{
		key:   keyAsString,
		cmd:   cmd,
		delta: delta,
		lmt:   lmt,
	}
}

func (r *RedisDelayedSyncPipelined) processSyncRes(cmdArgs syncArgs, cmdRes interface{}) error {
	key := cmdArgs.key
	vals := cmdRes.([]interface{})
	status := status(vals[0].(string))
	switch status {
	case OK:
		// handle normal sync
		if len(vals) > 1 {
			r.lastSyncedResetAt.Store(key, vals[1].(int64))
		}
	case AdjustLocal:
		diff := vals[1].(int64)
		remote := vals[2].(int64)
		cmdArgs.lmt.IncrementResetAtBy(diff)
		r.lastSyncedResetAt.Store(key, remote)
	case CorruptedRemote:
		lastSynced := vals[1].(int64)
		return r.executeCorruptedRemoteRecovery(key, cmdArgs.lmt, cmdArgs.delta, lastSynced)
	case Expired:
		r.lastSyncedResetAt.Delete(key)
	}

	return nil
}

func (r *RedisDelayedSyncPipelined) prefixKey(key string) string {
	return fmt.Sprintf("%s%s", r.redisPrefix, key)
}
